#include "XrdCephFileIOAdapter.hh"

#define MAX_ATTR_CHARS 128

int _get_object_name(std::string filename, size_t obj_idx, std::string& res){
  /* Writes full object name to buf. Returns 0 on success, or negative error code on error*/
  char object_suffix[18];
  int sp_bytes_written;
  sp_bytes_written = snprintf(object_suffix, sizeof(object_suffix), ".%016zx", obj_idx);
  if (sp_bytes_written >= (int) sizeof(object_suffix)) {
    //log_func((char*)"Can not fit object suffix into buffer for file %s -- too big\n", name.c_str());
    return -EFBIG;
  }

  try {
    res = filename + std::string(object_suffix);
  } catch (std::bad_alloc&) {
    //log_func((char*)"Can not create object string for file %s)", name.c_str());
    return -ENOMEM;
  }
  return 0;
}

XrdCephFileIOAdapter::XrdCephFileIOAdapter(const CephFile file) {
  name = file.name;
  pool = file.pool;
  userId = file.userId;
  nbStripes = file.nbStripes;
  stripeUnit = file.stripeUnit;
  objectSize = file.objectSize;
};

/*XrdCephFileIOAdapter::WriteRequestData::WriteRequestData(const char* input_buf, size_t len) {
  bl.append(input_buf, len);
}*/

XrdCephFileIOAdapter::CephReadOpData::CephReadOpData(const XrdCephFileIOAdapter::CephReadOpData& data) {
  cmpl = data.cmpl;
  read_buffers = std::list<ReadRequestData>(data.read_buffers);
}

XrdCephFileIOAdapter::XrdCephFileIOAdapter(logfunc_pointer logwrapper) {
  /**
   * Constructor.
   *
   * @param file_inf          Ceph file info pointer
   *
   */
  log_func = logwrapper;
}

XrdCephFileIOAdapter::~XrdCephFileIOAdapter() {
  /**
   * Destructor. Just clears dynamically allocated memroy.
   */
  clear();
}

void XrdCephFileIOAdapter::clear() {
  /**
   * Clear all dynamically alocated memory
   */
  read_operations.clear();
  //write_operations.clear();
}

int XrdCephFileIOAdapter::addReadRequest(size_t obj_idx, char* buffer, size_t size, off64_t offset) {
  /**
   * Prepare read request for a single ceph object. Private method.
   *
   * Method will allocate all (well, almost, except the string for the object name)
   * necessary objects to submit read/write request to ceph. To submit the requests use
   * `submit_and_wait_for_complete` method.
   *
   * @param obj_idx  number of the object (starting from zero) to read
   * @param buf      buffer, used to store/copy from results
   * @param size     number of bytes to read/write
   * @param offset   offset in bytes where the read should start. Note that the offset is local to the
   *                 ceph object. I.e. if offset is 0 and object number is 1, yo'll be reading from the
   *                 start of the second object, not from the begining of the file.
   *
   * @return         zero on success, negative error code on failure
   */
  int rc = 0;
  try{
    //When we start using C++17, the next two lines can be merged
    auto &op_data = read_operations[obj_idx];
    op_data.read_buffers.emplace_back(buffer);
    auto &buf = op_data.read_buffers.back();
    op_data.ceph_read_op.read(offset, size, &buf.bl, &buf.rc);
  } catch (std::bad_alloc&) {
    log_func((char*)"Memory allocation failed while reading file %s", name.c_str());
    return -ENOMEM;
  }
  return rc;
}

/*int XrdCephFileIOAdapter::submitWriteRequest(size_t obj_idx, char* buffer, size_t size, off64_t offset) {
  try{
    //When we start using C++17, the next two lines can be merged
    // FIX: check if object id is already in the map, and fail if it is
    auto &op_data = write_operations[obj_idx];
    op_data.read_buffers.emplace_back(buffer);
    auto &buf = op_data.read_buffers.back();
    op_data.ceph_read_op.read(offset, size, &buf.bl, &buf.rc);
  } catch (std::bad_alloc&) {
    log_func((char*)"Memory allocation failed while reading file %s", file_info->name.c_str());
    return -ENOMEM;
  }
  return rc;
}
}*/

/*int XrdCephFileIOAdapter::wait_for_write_complete() {
  int ret = 0;
  for (auto &buf_data: write_operations) {
    buf_data.second.cmpl.wait_for_complete();
    int ret = buf_data.second.cmpl.get_return_value();
    if (ret != 0) {
      log_func((char*)"Write for file %s failed\n", name.c_str());
      break;
    }
  }
  return ret;
}*/

int XrdCephFileIOAdapter::submit_reads_and_wait_for_complete(librados::IoCtx* context) {
  /**
   * Submit previously prepared read requests and wait for their completion
   *
   * To prepare read requests use `read/write` or `addRequest` methods.
   *
   * @return  zero on success, negative error code on failure
   *
   */

  for (auto &op_data: read_operations) {
    int rval = -1;
    size_t obj_idx = op_data.first;

    std::string obj_name;
    rval = get_object_name(obj_idx, obj_name);
    if (rval) {
      return rval;
    }

    context->aio_operate(obj_name, op_data.second.cmpl.use(), &op_data.second.ceph_read_op, 0);
  }

  for (auto &op_data: read_operations) {
    op_data.second.cmpl.wait_for_complete();
    int rval = op_data.second.cmpl.get_return_value();
    /*
     * Optimization is possible here: cancel all remaining read operations after the failure.
     * One way to do so is the following: add context as an argument to the `use` method of CmplPtr.
     * Then inside the class this pointer can be saved and used by the destructor to call
     * `aio_cancel` (and probably `wait_for_complete`) before releasing the completion.
     * Though one need to clarify whether it is necessary to cal `wait_for_complete` after
     * `aio_cancel` (i.e. may the status variable/bufferlist still be written to or not).
     */
    if (rval < 0) {
      log_func((char*)"Read of the object %ld for file %s failed", op_data.first, name.c_str());
      return rval;
    }
  }
  return 0;
}

ssize_t XrdCephFileIOAdapter::get_read_results() {
  /**
   * Copy the results of executed read requests from ceph's bufferlists to client's buffers
   *
   * Note that this method should be called only after the submission and completion of read
   * requests, i.e. after `submit_and_wait_for_complete` method.
   *
   * @return  cumulative number of bytes read (by all read operations) on success, negative
   *          error code on failure
   *
   */

  ssize_t res = 0;
  for (auto &op_data: read_operations) {
    for (auto &req_data: op_data.second.read_buffers) {
      if (req_data.rc < 0) {
        //Is it possible to get here?
        log_func((char*)"One of the reads failed with rc %d", req_data.rc);
        return req_data.rc;
      }
      req_data.bl.begin().copy(req_data.bl.length(), req_data.out_buf);
      res += req_data.bl.length();
    }
  }
  //We should clear used completions to allow new operations
  clear();
  return res;
}

int XrdCephFileIOAdapter::read(librados::IoCtx* context, void* out_buf, size_t req_size, off64_t offset) {
  return io_req_block_loop(context, out_buf, req_size, offset, NULL);
}

ssize_t XrdCephFileIOAdapter::write(librados::IoCtx* context, const char* input_buf, size_t req_size, off64_t offset) {
  IoFuncPtr write_method = [](std::string filename, librados::IoCtx* context, size_t start_block, const char* buf, size_t chunk_len, off64_t chunk_offset) {
    std::string obj_name;
    ssize_t rc = 0;
    rc = _get_object_name(filename, start_block, obj_name);
    if (rc) {
      return rc;
    }
    ceph::bufferlist bl;
    bl.append(buf, chunk_len);
    rc = context->write(obj_name, bl, chunk_len, chunk_offset);
    return rc;
  };
  return io_req_block_loop(context, (void*)input_buf, req_size, offset, write_method);
}

int XrdCephFileIOAdapter::io_req_block_loop(librados::IoCtx* context, void* buf, size_t req_size, off64_t offset, IoFuncPtr func) {
  /**
   * Declare a read or write operation for file.
   *
   * Read coordinates are global, i.e. valid offsets are from 0 to the <file_size> -1, valid request sizes
   * are from 0 to INF. Method can be called multiple times to declare multiple read
   * operations on the same file.
   *
   * @param buf        input (for write) or output (for read) buffer used for input/output data
   * @param req_size   number of bytes to process
   * @param offset     offset in bytes where the read/write op should start. Note that the offset is global,
   *                   i.e. refers to the whole file, not individual ceph objects
   *
   * @return  zero on success, negative error code on failure
   *
   */

  if (req_size == 0) {
    log_func((char*)"Zero-length read request for file %s, probably client error", name.c_str());
    return 0;
  }

  char* const buf_start_ptr = (char*) buf;

  //The amount of bytes that is yet to be read
  size_t to_process = req_size;
  //block means ceph object here
  size_t start_block = offset / objectSize;
  size_t buf_pos = 0;
  size_t chunk_start = offset % objectSize;

  while (to_process > 0) {
    size_t chunk_len = std::min(to_process, (size_t) (objectSize - chunk_start));

    if (buf_pos >= req_size) {
      log_func((char*)"Internal bug! Attempt to read %lu data for block (%lu, %lu) of file %s\n", buf_pos, offset, req_size, name.c_str());
      return -EINVAL;
    }

    int rc = -1;
    if (NULL == func) {
      rc = addReadRequest(start_block, buf_start_ptr + buf_pos, chunk_len, chunk_start);
      if (rc < 0) {
        log_func((char*)"Unable to submit async read request, rc=%d, file=%s\n", rc, name.c_str());
        return rc;
      }
    } else {
      /*std::string obj_name;
      ssize_t rc = 0;
      rc = _get_object_name(name, start_block, obj_name);
      if (rc) {
        return rc;
      }
      ceph::bufferlist bl;
      bl.append((const char*)buf, chunk_len);
      log_func((char*)"Writing to object=%s, %d, %d\n", name.c_str(), chunk_len, chunk_start);
      rc = context->write(obj_name, bl, chunk_len, chunk_start);*/
      rc = func(name, context, start_block, buf_start_ptr + buf_pos, chunk_len, chunk_start);
      if (rc < 0) {
        log_func((char*)"Unable to submit custom request, rc=%d, file=%s\n", rc, name.c_str());
        return rc;
      }
    }

    buf_pos += chunk_len;

    start_block++;
    chunk_start = 0;
    if (chunk_len > to_process) {
      log_func((char*)"Internal bug! Process %lu bytes, more than expected %lu bytes for block (%lu, %lu) of file %s\n", chunk_len, to_process, offset, req_size, name.c_str());
      return -EINVAL;
    }
    to_process = to_process - chunk_len;
  }
  return 0;
}


/*ssize_t XrdCephFileIOAdapter::write(const void* in_buf, size_t req_size, off64_t offset) {
  /**
   * Synchronously write file data.
   *
   * Read coordinates are global, i.e. valid offsets are from 0 to the <file_size> -1, valid request sizes
   * are from 0 to INF.
   *
   * @param in_buf     input buffer, where data to be written is stored
   * @param req_size   number of bytes to write
   * @param offset     offset in bytes where the write should start. Note that the offset is global,
   *                   i.e. refers to the whole file, not individual ceph objects
   *
   * @return  zero on success, negative error code on failure
   *
   * /

  if (req_size == 0) {
    log_func((char*)"Zero-length write request for file %s, probably client error", file_ref->name.c_str());
    return 0;
  }

  char* buf_ptr = (char*) in_buf;

  size_t object_size = file_ref->objectSize;
  //The amount of bytes that is yet to be read
  size_t to_write = req_size;
  //block means ceph object here
  size_t cur_block = offset / object_size;
  size_t chunk_offset = offset % object_size;
  //size_t buf_pos = 0;
  //char block_suffix[18];
  size_t total_bytes_written = 0;

  while (to_write > 0) {
    size_t chunk_len = std::min(object_size - chunk_offset, to_write);
    int res =  write_to_object(buf_ptr, cur_block, chunk_len, chunk_offset);
    if (0 == res) {
      buf_ptr += chunk_len;
      total_bytes_written += chunk_len;
      cur_block += 1;
      chunk_offset = 0;
      to_write -= chunk_len;
    } else {
      return res;
    }
  }
  return total_bytes_written;
}

int XrdCephFileIOAdapter::write_to_object(const char* buf_ptr, size_t cur_block, size_t chunk_len, size_t chunk_offset) {
  std::string obj_name;
  if (int res = get_object_name(cur_block, obj_name)) {
    return res;
  }
  ceph::bufferlist bl;
  bl.append((const char*)buf_ptr, chunk_len);
  return context->write(obj_name.c_str(), bl, chunk_len, chunk_offset);
}*/

int XrdCephFileIOAdapter::setxattr(librados::IoCtx* context, const char* attr_name, const char *input_buf, size_t len) {
  std::string obj_name;
  int rc;
  rc = get_object_name(0, obj_name);
  if (rc) {
    return rc;
  }
  ceph::bufferlist bl;
  bl.append((const char*)input_buf, len);
  rc = context->setxattr(obj_name.c_str(), attr_name, bl);
  if (rc) {
    log_func((char*)"Can not get %s attr for for file %s -- too big\n", attr_name, name.c_str());
  }
  return rc;
}

int XrdCephFileIOAdapter::log_xattrs(librados::IoCtx* context) {
  std::string obj_name;
  int rc;
  rc = get_object_name(0, obj_name);
  if (rc) {
    return rc;
  }
  std::map<std::string, ceph::bufferlist> dict;
  rc = context->getxattrs(obj_name, dict);
  for (auto i: dict) {
    log_func((char*)"got attr %s\n", i.first);
  }
  return 0;
}

ssize_t XrdCephFileIOAdapter::getxattr(librados::IoCtx* context, const char* attr_name, char *output_buf, size_t buf_size) {
  int rc;
  //rc = log_xattrs(context);
  std::string obj_name;
  rc = get_object_name(0, obj_name);
  if (rc) {
    return rc;
  }
  ceph::bufferlist bl;
  rc = context->getxattr(obj_name, attr_name, bl);
  if (rc < 0) {
    log_func((char*)"Can not get %s attr for file %s -- %d\n", attr_name, obj_name.c_str(), rc);
    return rc;
  }
  size_t to_copy = bl.length();
  if (to_copy > buf_size - 1) {
    log_func((char*)"Can not fit %s attr of file %s to %lu bytes buffer -- too big (%lu+1 bytes)\n", attr_name, name.c_str(), buf_size, to_copy);
    return -E2BIG;
  } 
  bl.begin().copy(bl.length(), output_buf);
  //Just in case, add null-terminator
  output_buf[to_copy] = '\0';
  //  log_func((char*)"Got %s attr of file %s : %s\n", attr_name, name.c_str(), output_buf);

  return to_copy;
}

ssize_t XrdCephFileIOAdapter::get_numeric_attr(librados::IoCtx* context, const char* attr_name) {
  char tmp_buf[MAX_ATTR_CHARS];
  int rc = getxattr(context, attr_name, tmp_buf, MAX_ATTR_CHARS);
  if (rc < 0) {
    return rc;
  }
  return atoll(tmp_buf);
}

ssize_t XrdCephFileIOAdapter::get_size(librados::IoCtx* context) {
  ssize_t size = get_numeric_attr(context, "striper.size");
  return size;
}

ssize_t XrdCephFileIOAdapter::get_object_size(librados::IoCtx* context) {
  ssize_t obj_size = get_numeric_attr(context, "striper.layout.object_size");
  return obj_size;
}


int XrdCephFileIOAdapter::remove(librados::IoCtx* context) {
  return remove_objects(context);
}

int XrdCephFileIOAdapter::truncate(librados::IoCtx* context) {
  int rc = remove_objects(context, true);
  if (rc != 0) {
    return rc;
  }
  std::string obj_name;
  rc = get_object_name(0, obj_name);
  if (rc != 0) {
    return rc;
  }
  rc = context->trunc(obj_name, 0);
  if (rc != 0) {
    log_func((char*)"Can not truncate first object of the file %s:  %d\n", name.c_str(), rc);
  } 
  return rc;
}

int XrdCephFileIOAdapter::remove_objects(librados::IoCtx* context, bool keep_first) {
  //CmplPtr* completions;
  ssize_t file_size = get_size(context);
  ssize_t object_size = get_object_size(context);
  size_t obj_count = 0;
  int rc = 0;
  if (file_size < 0) {
    log_func((char*)"Can not delete %s -- failed to get file size", name.c_str());
    return (int)file_size;
  } else if (0 == file_size) {
    obj_count = 1; 
  } else {
    obj_count = (file_size-1) / object_size + 1;
  }

  ssize_t end_object = keep_first ? 1 : 0;
  std::list<CmplPtr> completions;
  for (ssize_t i=obj_count-1; i>=end_object; i--) {
    std::string obj_name;
    rc = get_object_name(i, obj_name);
    if (rc < 0) {
      return rc;
    }
    completions.emplace_back();
    context->aio_remove(obj_name, completions.back().use());
  }
  rc = 0;
  for (auto& c: completions) {
    c.wait_for_complete();
    rc = std::min(rc, c.get_return_value());
    if (rc < 0 ) {
      log_func((char*)"Can not delete %s -- object deletion failed %d\n", name.c_str(),  rc);
    }
  }
  return rc;
}

int XrdCephFileIOAdapter::get_object_name(size_t obj_idx, std::string& res){
  /* Writes full object name to buf. Returns 0 on success, or negative error code on error*/
  return _get_object_name(name, obj_idx, res);
}
