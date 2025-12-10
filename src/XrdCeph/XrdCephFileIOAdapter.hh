#include <string>
#include <map>
#include <list>
#include <tuple>
#include <rados/librados.hpp>

#include "XrdSys/XrdSysPthread.hh"

#ifndef _XRD_CEPH_IO_FILE
#define _XRD_CEPH_IO_FILE

typedef ssize_t (*IoFuncPtr)(std::string, librados::IoCtx*, size_t, const char*, size_t, off64_t);

/*enum OpType {
  OP_READ,
  OP_WRITE
};*/

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
  typedef void (*logfunc_pointer) (char *, ...);

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
  ~XrdCephFileIOAdapter();

  void clear();
  int wait_for_write_complete();
  int submit_reads_and_wait_for_complete(librados::IoCtx* context);
  ssize_t get_read_results();
  //int read(void *out_buf, size_t size, off64_t offset);
  //ssize_t write(const void *in_buf, size_t size, off64_t offset);
  int read(librados::IoCtx* context, void *output_buf, size_t size, off64_t offset);
  ssize_t write(librados::IoCtx* context, const char *input_buf, size_t size, off64_t offset);
  logfunc_pointer log_func; 

  private:
  //Completion pointer
  class CmplPtr {
    librados::AioCompletion *ptr;
    bool used = false;
    public:
    CmplPtr() {
      ptr = librados::Rados::aio_create_completion();
      if (NULL == ptr) {
        throw std::bad_alloc();
      }
    }
    ~CmplPtr() {
      if (used) {
        this->wait_for_complete();
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
  };

  //Data for an individual read -- ceph's buffer, client's buffer and return code
  struct ReadRequestData {
    ceph::bufferlist bl;
    char* out_buf;
    int rc;
    ReadRequestData(char* output_buf): out_buf(output_buf), rc(-1) {};
  };

  //All data neaded for individual read operation
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
    WriteRequestData(const char* input_buf, size_t len);
  };

  //int write_to_object(const char* buf_ptr, size_t cur_block, size_t chunk_len, size_t chunk_offset);
  int get_object_name(size_t obj_idx, std::string& res);
  int addReadRequest(size_t obj_idx, char *buffer, size_t size, off64_t offset);
  int io_req_block_loop(librados::IoCtx* context, void* buf, size_t req_size, off64_t offset, IoFuncPtr func);

  //map { <object_number> : <CephOpData> }
  std::map<size_t, CephReadOpData> read_operations;
  std::map<size_t, WriteRequestData> write_operations;

  //CephFile* file_info;
};
#endif
