#include <string>
#include <map>
#include <list>
#include <tuple>
#include <atomic>
#include <rados/librados.hpp>

#include "XrdSys/XrdSysPthread.hh"

#define MAX_ATTR_CHARS 128
#define MAX_FILENAME_CHARS 4096
#define ALLOW_PARTIAL_OBJECT_WRITES 1

#ifndef _XRD_CEPH_IO_FILE
#define _XRD_CEPH_IO_FILE

typedef std::atomic<size_t> atomic_size_t;
typedef std::atomic<ssize_t> atomic_ssize_t;

enum OpType {
  OP_READ,
  OP_READ_ASYNC,
  OP_WRITE_SYNC,
  OP_WRITE_ASYNC,
  OP_WRITE_IMPLICIT_ASYNC
};

struct readCallbackWrapperArg {
  char* buf = NULL;
  atomic_size_t* total_bytes_read = 0;
  atomic_ssize_t* rc = 0;
  size_t total_read_size = 0;
  ceph::bufferlist bl;
  librados::callback_t callback = NULL;
  void* callback_arg = NULL;
};

struct CephFile {
  std::string name;
  std::string pool;
  std::string userId;
  unsigned int nbStripes;
  unsigned long long stripeUnit;
  unsigned long long objectSize;
};

/// small structs to store file metadata
struct CephFileRef : CephFile {
  int flags;
  mode_t mode;
  uint64_t current_offset;
  // This mutex protects against parallel updates of the stats.
  XrdSysMutex statsMutex;
  uint64_t maxOffsetWritten;
  uint64_t bytesAsyncWritePending;
  uint64_t bytesWritten;
  unsigned rdcount;
  unsigned wrcount;
  unsigned asyncRdStartCount;
  unsigned asyncRdCompletionCount;
  unsigned asyncWrStartCount;
  unsigned asyncWrCompletionCount;
  ::timeval lastAsyncSubmission;
  double longestAsyncWriteTime;
  double longestCallbackInvocation;
};

class XrdCephFileIOAdapter: public CephFileRef {
  //typedef std::tuple<ceph::bufferlist*, char*, int*> ReadOpData;
  typedef void (*logfunc_pointer) (char *, va_list args);

  /**
   * Class is used to execute io operations against rados striper files *without* usage of rados striper.
   * Reads are based on ceph read operations.
   *
   * The interface is similar to the one that ceph's read operation objects has:
   * 1. Instantiate the object.
   * 2. Declare read operations using 'read' method, providing the output buffers, offset and length.
   * 3. Submitn operation and wait for results using 'submit_and_wait_for_complete' method.
   * 4. Copy results to buffers with 'get_results' method.
   *
   * WARNING: there is no copy/move constructor in the class, so do not use temporary objects for initialization
   * (i.e. something like `XrdCephFileIOAdapter io = XrdCephFileIOAdapter(...);` will not work, use `XrdFileIOAdapter io(...);` instead).
   */
  public:
  XrdCephFileIOAdapter(logfunc_pointer ptr=NULL);
  XrdCephFileIOAdapter(const CephFile file, logfunc_pointer ptr=NULL);
  ~XrdCephFileIOAdapter();

  void clear();
  //int wait_for_write_complete();
  int submit_reads(librados::IoCtx* context);
  int wait_for_read_complete();
  int submit_reads_and_wait_for_complete(librados::IoCtx* context);
  int wait_for_write_complete();
  ssize_t get_read_results();
  //int read(void *out_buf, size_t size, off64_t offset);
  //ssize_t write(const void *in_buf, size_t size, off64_t offset);
  int read(librados::IoCtx* context, void *output_buf, size_t size, off64_t offset);
  int read_aio(librados::IoCtx* context, void* out_buf, size_t req_size, off64_t offset, void* arg, librados::callback_t callback);
  ssize_t read_block_async(librados::IoCtx* context, size_t block_num, size_t req_size, off64_t offset,  readCallbackWrapperArg* arg);
  ssize_t write_block_sync(librados::IoCtx* context, size_t block_num, const char* input_buf, size_t req_size, off64_t offset);
  ssize_t write_block_async(librados::IoCtx* context, size_t block_num, const char* input_buf, size_t req_size, off64_t offset, void* arg, librados::callback_t callback);
  ssize_t write_aio(librados::IoCtx* context, const char* input_buf, size_t req_size, off64_t offset, void* arg, librados::callback_t callback);
  ssize_t write(librados::IoCtx* context, const char *input_buf, size_t size, off64_t offset);
  int setxattr(librados::IoCtx* context, const char* attr_name, const char *input_buf, size_t len);
  ssize_t getxattr(librados::IoCtx* context, const char* attr_name, char *output_buf, size_t len);
  int getxattrs(librados::IoCtx* context, std::map<std::string, ceph::bufferlist>& dict);
  int rmxattr(librados::IoCtx* context, const char* name);
  int remove(librados::IoCtx* context);
  int truncate(librados::IoCtx* context);
  int lock(librados::IoCtx* context,  time_t lock_timeout=6*3600);
  int unlock(librados::IoCtx* context);
  int stat(librados::IoCtx* context, uint64_t* size, time_t* mtime);
  std::string lock_cookie;
  logfunc_pointer log_func;

  private:
  bool allow_implicit_async_writes = false;

  //Completion pointer
  class CmplPtr {
    librados::AioCompletion *ptr;
    bool used = false;
    public:
    CmplPtr(void* arg=NULL, librados::callback_t callback=NULL) {
      ptr = librados::Rados::aio_create_completion(arg, callback);
      if (NULL == ptr) {
        throw std::bad_alloc();
      }
    }
    ~CmplPtr() {
      if (used) {
        ptr->wait_for_complete();
      }
      ptr->release();
    }
    void wait_for_complete() {
      ptr->wait_for_complete();
    }
    int get_return_value() {
      return ptr->get_return_value();
    }
    librados::AioCompletion* use() {
      //If the object was converted to AioCompletion, we suppose it was passed to
      //the read or write operation, and therefore set the flag.
      used = true;
      return ptr;
    }
    librados::AioCompletion* access() {
      //Access PTR but do not record it. Use with care!
      return ptr;
    }

  };

  /*
  class MultiAIOCmplPtr {
    std::list<CmplPtr> completion_list;
    size_t ops_executed = 0;
    size_t ops_registered = 0;
    bool all_submitted = false;
    void* c_arg = NULL;
    librados::callback_t g_callback = NULL;
    MultiAIOCmplPtr(void* arg, librados::callback_t callback) {
      g_callback = callback;
      c_arg = arg;
    }

    static void callback_wrapper(void* arg) {
      MultiAIOCmplPtr* ptr = (MultiAIOCmplPtr*) arg;
      if (ptr->ops_executed == ptr->ops_registered && all_submitted) {
	ptr->g_callback(ptr->c_arg);
      }
    }
    librados::AioCompletion* use() {
      completion_list.emplace_back(this, callback_wrapper);
      return completion_list.back().use();
    }
    void submissionDone() {
      all_submitted = true;
    }
  };*/

  //Data for an individual read -- ceph's buffer, client's buffer and return code
  struct ReadRequestData {
    ceph::bufferlist bl;
    char* out_buf;
    int rc;
    ReadRequestData(char* output_buf): out_buf(output_buf), rc(-1) {};
  };

  //All data neaded for individual read operation (i.e. vector read for a ceph object)
  struct CephReadOpData {
    librados::ObjectReadOperation ceph_read_op;
    CmplPtr cmpl;
    std::list<ReadRequestData> read_buffers;
    CephReadOpData(const CephReadOpData&);
    CephReadOpData(){};
  };

  //All data needed for an individual write request -- ceph's buffer and completion
  struct WriteRequestData {
    ceph::bufferlist bl;
    CmplPtr cmpl;
    size_t obj;
    size_t offset;
    WriteRequestData(const char* input_buf, size_t len, size_t offset=0, size_t obj_num=0, void* arg=NULL, librados::callback_t g_callback = NULL);
  };

  //int write_to_object(const char* buf_ptr, size_t cur_block, size_t chunk_len, size_t chunk_offset);
  std::string get_object_name(size_t obj_idx);
  int addReadRequest(size_t obj_idx, char *buffer, size_t size, off64_t offset);
  int io_req_block_loop(librados::IoCtx* context, void* buf, size_t req_size, off64_t offset, OpType op_type, void* arg=NULL, librados::callback_t callback=NULL);
  int remove_objects(librados::IoCtx* context, bool keep_first=false);
  ssize_t get_numeric_attr(librados::IoCtx* context, const char* attr_name);
  ssize_t get_size(librados::IoCtx* context);
  ssize_t get_object_size(librados::IoCtx* context);
  std::string lock_name = "striper.lock";

  void log(char* format, ...);

  //map { <object_number> : <CephOpData> }
  std::map<size_t, CephReadOpData> read_operations;
  std::list<WriteRequestData> write_operations;

  //CephFile* file_info;
};
#endif
