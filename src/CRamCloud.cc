/* Copyright (c) 2010-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * This file provides a C wrapper around the RAMCloud client code.
 */

#include "RamCloud.h"
#include "CRamCloud.h"
#include "ClientException.h"
#include "Logger.h"

using namespace RAMCloud;

/**
 * Wrapper structure for C clients.
 */
struct rc_client {
    RamCloud* client;
};

/**
 * Stores user-provided information and the the RAMCloud object buffer to
 * transfer the response of a multi read operation into C managed memory.
 */
struct rc_multiReadHelper {
    rc_multiReadHelper(void *buf, uint32_t maxLength, uint32_t *actualLength)
        : buf(buf)
        , maxLength(maxLength)
        , actualLength(actualLength)
        , value()
        { *actualLength = 0; }
    void *buf;   ///< A user-provided buffer to store the value of an object
    uint32_t maxLength;   ///< The size of the buffer *buf in bytes
    uint32_t *actualLength;  ///< The actual size of the value in bytes
    Tub<ObjectBuffer> value;  ///< The value buffer as required by the C++ API

    DISALLOW_COPY_AND_ASSIGN(rc_multiReadHelper);
};

/**
 * Create a new client connection to a RAMCloud cluster.
 *
 * \param locator
 *      Information about how to connect to the cluster coordinator;
 *      see the constructor for RamCloud for details.
 * \param clusterName
 *      Name of the cluster.
 * \param[out] newClient
 *      A pointer to the new client connection is returned here
 *      if the return value is STATUS_OK.  This pointer is passed
 *      to other "rc_" functions to invoke RAMCloud operations.
 *
 * \return
 *      STATUS_OK or STATUS_COULDNT_CONNECT or STATUS_INTERNAL_ERROR.
 */
Status rc_connect(const char* locator, const char* clusterName,
        struct rc_client** newClient)
{
    struct rc_client* client = new rc_client;
    try {
        client->client = new RamCloud(locator, clusterName);
    } catch (CouldntConnectException& e) {
        delete client;
        *newClient = NULL;
        return e.status;
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
        return STATUS_INTERNAL_ERROR;
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
        return STATUS_INTERNAL_ERROR;
    }

    *newClient = client;
    return STATUS_OK;
}

/**
 * Create a C rc_client based on an existing C++ RamCloudClient.  Intended
 * primarily for testing.
 *
 * \param existingClient
 *      An existing RamCloudClient object that will be used for communication
 *      with the server.
 * \param[out] newClient
 *      A pointer to the new client connection is returned here.
 *      This pointer is passed to other "rc_" functions to invoke
 *      RAMCloud operations.
 *
 * \return
 *      STATUS_OK or STATUS_COULDNT_CONNECT or STATUS_INTERNAL_ERROR.
 */
Status rc_connectWithClient(
        struct RamCloud* existingClient,
        struct rc_client** newClient)
{
    struct rc_client* client = new rc_client;
    client->client = existingClient;
    *newClient = client;
    return STATUS_OK;
}

/**
 * End a connection to a RAMCloud cluster and delete the client object.
 *
 * \param client
 *      Handle for the RAMCloud connection.  This is freed by this
 *      function, so it should not be used again after the function
 *      returns.
 */
void rc_disconnect(struct rc_client* client) {
    delete client->client;
    delete client;
}

// Most of the methods below are all just wrappers around the corresponding
// RamCloudClient methods, except for the following differences:
// * RPC requests here return Status values, whereas the C++ methods
//   generate exceptions.
// * Anything returned as result by a C++ method is returned by a
//   pointer argument here.
// See the documentation in RamCloudClient.cc for details.

Status
rc_createTable(struct rc_client* client, const char* name, uint32_t serverSpan)
{
    try {
        client->client->createTable(name, serverSpan);
    } catch (ClientException& e) {
        return e.status;
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
        return STATUS_INTERNAL_ERROR;
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
        return STATUS_INTERNAL_ERROR;
    }

    return STATUS_OK;
}

Status
rc_dropTable(struct rc_client* client, const char* name)
{
    try {
        client->client->dropTable(name);
    } catch (ClientException& e) {
        return e.status;
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
        return STATUS_INTERNAL_ERROR;
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
        return STATUS_INTERNAL_ERROR;
    }

    return STATUS_OK;
}

/**
 * Return the completion status from the most recent RPC.
 *
 * \return
 *      See above.
 */
Status
rc_getStatus(struct rc_client* client) {
    return client->client->status;
}

Status
rc_getTableId(struct rc_client* client, const char* name,
        uint64_t* tableId)
{
    try {
        *tableId = client->client->getTableId(name);
    } catch (ClientException& e) {
        return e.status;
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
        return STATUS_INTERNAL_ERROR;
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
        return STATUS_INTERNAL_ERROR;
    }

    return STATUS_OK;
}

/**
 * Similar to RamCloudClient::read, except copies the return value out to a
 * fixed-length buffer rather than returning a Buffer object.
 *
 * \param client
 *      Handle for the RAMCloud connection.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned
 *      here.
 * \param[out] buf
 *      The contents of the desired object are copied to this location.
 * \param[out] maxLength
 *      Number of bytes of space available at buf: if the object is
 *      larger than this that only this many bytes will be copied to
 *      buf.
 * \param[out] actualLength
 *      The total size of the object is stored here; this may be
 *      larger than maxLength.
 *
 * \return
 *      0 means success, anything else indicates an error.
 */
Status
rc_read(struct rc_client* client, uint64_t tableId,
        const void* key, uint16_t keyLength,
        const struct RejectRules* rejectRules, uint64_t* version,
        void* buf, uint32_t maxLength, uint32_t* actualLength)
{
    Buffer result;
    try {
        client->client->read(tableId, key, keyLength, &result, rejectRules,
                version);
        *actualLength = result.size();
        uint32_t bytesToCopy = *actualLength;
        if (bytesToCopy > maxLength) {
            bytesToCopy = maxLength;
        }
        result.copy(0, bytesToCopy, buf);
    } catch (ClientException& e) {
        *actualLength = 0;
        return e.status;
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
        return STATUS_INTERNAL_ERROR;
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
        return STATUS_INTERNAL_ERROR;
    }

    return STATUS_OK;
}

Status
rc_remove(struct rc_client* client, uint64_t tableId,
          const void* key, uint16_t keyLength,
          const struct RejectRules* rejectRules, uint64_t* version)
{
    try {
        client->client->remove(tableId, key, keyLength, rejectRules, version);
    } catch (ClientException& e) {
        return e.status;
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
        return STATUS_INTERNAL_ERROR;
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
        return STATUS_INTERNAL_ERROR;
    }

    return STATUS_OK;
}

Status
rc_write(struct rc_client* client, uint64_t tableId,
         const void* key, uint16_t keyLength,
         const void* buf, uint32_t length,
         const struct RejectRules* rejectRules,
         uint64_t* version)
{
    try {
        client->client->write(tableId, key, keyLength, buf, length, rejectRules,
                version);
    } catch (ClientException& e) {
        return e.status;
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
        return STATUS_INTERNAL_ERROR;
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
        return STATUS_INTERNAL_ERROR;
    }

    return STATUS_OK;
}

// Multi-op calls require three steps from the C user.  A first bunch of calls
// constructs the C++ Multi{Read,Write,Remove}Object objects, the second
// step issues the multi-op RPC, and in a last step a bunch of calls destructs
// the C++ MultiOpObject objects.  To the C user, the MultiOpObjects are just
// void pointers.  The size of the objects, which needs to be allocated by the
// C user, is returned by rc_multiOpSizeOf.

/**
 * Constructs a MultiReadObject and the necessary additional fields to transfer
 * the returned ObjectBuffer to a C void buffer.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] buf
 *      The contents of the desired object are copied to this location.
 * \param[out] maxLength
 *      Number of bytes of space available at buf: if the object is
 *      larger than this that only this many bytes will be copied to
 *      buf.
 * \param[out] actualLength
 *      The total size of the object is stored here; this may be
 *      larger than maxLength.
 * \param where
 *      The memory location for creating the objects. The location needs to
 *      be large enough to hold at least rc_multiOpSizeOf(MULTI_OP_READ) bytes.
 */
void
rc_multiReadCreate(uint64_t tableId,
                   const void *key, uint16_t keyLength,
                   void* buf, uint32_t maxLength, uint32_t *actualLength,
                   void *where)
{
    rc_multiReadHelper *mReadHelper = reinterpret_cast<rc_multiReadHelper *>
        (reinterpret_cast<char *>(where) + sizeof(MultiReadObject));
    new(mReadHelper) rc_multiReadHelper(buf, maxLength, actualLength);
    new(where) MultiReadObject(tableId, key, keyLength, &(mReadHelper->value));
}

/**
 * Constructs a MultiRemoveObject.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the remove
 *      should be aborted with an error.
 * \param where
 *      The memory location for creating the objects. The location needs to
 *      be large enough to hold at least rc_multiOpSizeOf(MULTI_OP_REMOVE)
 *      bytes.
 */
void
rc_multiRemoveCreate(uint64_t tableId,
                     const void* key, uint16_t keyLength,
                     const struct RejectRules* rejectRules,
                     void* where)
{
    new(where) MultiRemoveObject(tableId, key, keyLength, rejectRules);
}

/**
 * Constructs a MultiWriteObject.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \param buf
 *      Points to the memory location of the value.
 * \param length
 *      Size in bytes of the value.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the remove
 *      should be aborted with an error.
 * \param where
 *      The memory location for creating the objects. The location needs to
 *      be large enough to hold at least rc_multiOpSizeOf(MULTI_OP_REMOVE)
 *      bytes.
 */
void
rc_multiWriteCreate(uint64_t tableId,
                    const void* key, uint16_t keyLength,
                    const void* buf, uint32_t length,
                    const struct RejectRules* rejectRules,
                    void *where)
{
    new(where) MultiWriteObject(tableId, key, keyLength,
                                 buf, length, rejectRules);
}

/**
 * Frees resources of a multi-op object that have been previously allocated
 * with rc_multiReadCreate / rc_multiWriteCreate / rc_multiRemoveCreate.  Does
 * not free the memory itself but only calls the C++ destructors.
 *
 * \param multiOpObject
 *      Points to a memory location that has been previously the where parameter
 *      in a call to rc_multi{Read,Write,Remove}Create.
 * \param type
 *      Type of the multi-op object multiOpObject.
 */
void
rc_multiOpDestroy(void *multiOpObject, MultiOp type) {
    rc_multiReadHelper *mReadHelper;
    switch (type) {
        case MULTI_OP_READ:
            reinterpret_cast<MultiReadObject *>
                (multiOpObject)->~MultiReadObject();
            mReadHelper = reinterpret_cast<rc_multiReadHelper *>(
                reinterpret_cast<char *>(multiOpObject) +
                sizeof(MultiReadObject));
            mReadHelper->~rc_multiReadHelper();
            break;
        case MULTI_OP_WRITE:
            reinterpret_cast<MultiWriteObject *>
                (multiOpObject)->~MultiWriteObject();
            break;
        case MULTI_OP_REMOVE:
            reinterpret_cast<MultiRemoveObject *>
                (multiOpObject)->~MultiRemoveObject();
            break;
        default:
            RAMCLOUD_DIE("Destruct unknown multi-op object");
    }
}

/**
 * Get the status of a multi-op object after a call
 * to rc_multi{Read,Write,Remove}.
 *
 * \param multiOpObject
 *      Points to a memory location that has been previously the where parameter
 *      in a call to rc_multi{Read,Write,Remove}Create.
 * \param type
 *      Type of the multi-op object multiOpObject.
 */
Status
rc_multiOpStatus(const void *multiOpObject, MultiOp type) {
    return reinterpret_cast<const MultiOpObject *>(multiOpObject)->status;
}

/**
 * Returns the size in bytes that the C user must allocate for a call to
 * rc_multi{Read,Write,Remove}Create.
 *
 * \param type
 *      Type of the desired multi-op object.
 */
uint16_t
rc_multiOpSizeOf(MultiOp type) {
    switch (type) {
        case MULTI_OP_READ:
            return sizeof(MultiReadObject) + sizeof(rc_multiReadHelper);
        case MULTI_OP_WRITE:
            return sizeof(MultiWriteObject);
        case MULTI_OP_REMOVE:
            return sizeof(MultiRemoveObject);
        default:
            RAMCLOUD_DIE("Get size of unknown multi-op object");
    }
}

/**
 * Get the version of a multi-op object after a call
 * to rc_multi{Read,Write,Remove}.
 *
 * \param multiOpObject
 *      Points to a memory location that has been previously the where parameter
 *      in a call to rc_multi{Read,Write,Remove}Create.
 * \param type
 *      Type of the multi-op object multiOpObject.
 */
uint64_t
rc_multiOpVersion(const void *multiOpObject, MultiOp type) {
  switch (type) {
      case MULTI_OP_READ:
          return reinterpret_cast<const MultiReadObject *>
                     (multiOpObject)->version;
          break;
      case MULTI_OP_WRITE:
          return reinterpret_cast<const MultiWriteObject *>
                     (multiOpObject)->version;
          break;
      case MULTI_OP_REMOVE:
          return reinterpret_cast<const MultiRemoveObject *>
                     (multiOpObject)->version;
          break;
      default:
          RAMCLOUD_DIE("Get version from unknown multi-op object");
  }
}

/**
 * Issues a read on multiple objects.
 *
 * \param client
 *      Handle for the RAMCloud connection.
 * \param requests
 *      An array of pointers to memory structures that have been each created
 *      with rc_multiReadCreate.
 * \param numRequests
 *      The size of the requests array.
 */
void
rc_multiRead(struct rc_client* client,
              void **requests, uint32_t numRequests)
{
    MultiReadObject **read_requests =
        reinterpret_cast<MultiReadObject **>(requests);
    try {
        client->client->multiRead(read_requests, numRequests);
        for (uint32_t i = 0; i < numRequests; ++i) {
            rc_multiReadHelper *mReadHelper =
                reinterpret_cast<rc_multiReadHelper *>
                    (reinterpret_cast<char *>(requests[i]) +
                    sizeof(MultiReadObject));
            *(mReadHelper->actualLength) = 0;
            if (read_requests[i]->status != STATUS_OK) {
                continue;
            }

            uint16_t value_offset;
            bool retval = mReadHelper->value->getValueOffset(&value_offset);
            if (!retval) {
                read_requests[i]->status = STATUS_INVALID_OBJECT;
                continue;
            }
            *(mReadHelper->actualLength) =
                mReadHelper->value->size() - value_offset;
            uint32_t nBytes = *(mReadHelper->actualLength) ;
            if (nBytes > mReadHelper->maxLength) {
                nBytes = mReadHelper->maxLength;
            }
            mReadHelper->value->copy(value_offset, nBytes, mReadHelper->buf);
        }
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
    }
}

/**
 * Issues a write of multiple objects.
 *
 * \param client
 *      Handle for the RAMCloud connection.
 * \param requests
 *      An array of pointers to memory structures that have been each created
 *      with rc_multiWriteCreate.
 * \param numRequests
 *      The size of the requests array.
 */
void
rc_multiRemove(struct rc_client* client,
               void **requests, uint32_t numRequests)
{
    MultiRemoveObject **remove_requests =
        reinterpret_cast<MultiRemoveObject **>(requests);
    try {
        client->client->multiRemove(remove_requests, numRequests);
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
    }
}

/**
 * Issues a remove of multiple objects.
 *
 * \param client
 *      Handle for the RAMCloud connection.
 * \param requests
 *      An array of pointers to memory structures that have been each created
 *      with rc_multiRemoveCreate.
 * \param numRequests
 *      The size of the requests array.
 */
void
rc_multiWrite(struct rc_client* client,
              void **requests, uint32_t numRequests)
{
    MultiWriteObject **write_requests =
        reinterpret_cast<MultiWriteObject **>(requests);
    try {
        client->client->multiWrite(write_requests, numRequests);
    }
    catch (std::exception& e) {
        RAMCLOUD_LOG(ERROR, "An unhandled C++ Exception occurred: %s",
                e.what());
    } catch (...) {
        RAMCLOUD_LOG(ERROR, "An unknown, unhandled C++ Exception occurred");
    }
}

Status
rc_testing_kill(struct rc_client* client, uint64_t tableId,
                const void* key, uint16_t keyLength)
{
    try {
        client->client->testingKill(tableId, key, keyLength);
    } catch (const ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

Status
rc_testing_get_server_id(struct rc_client* client,
                         uint64_t tableId,
                         const void* key,
                         uint16_t keyLength,
                         uint64_t* serverId)
{
    try {
        *serverId = client->client->testingGetServerId(tableId, key, keyLength);
    } catch (const ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

Status
rc_testing_get_service_locator(struct rc_client* client,
                               uint64_t tableId,
                               const void* key,
                               uint16_t keyLength,
                               char* locatorBuffer,
                               size_t bufferLength)
{
    try {
        string locator =
            client->client->testingGetServiceLocator(tableId, key, keyLength);
        strncpy(locatorBuffer, locator.data(), bufferLength);
        if (bufferLength > 0)
            locatorBuffer[bufferLength - 1] = '\0';
    } catch (const ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

Status
rc_testing_fill(struct rc_client* client, uint64_t tableId,
                const void* key, uint16_t keyLength,
                uint32_t objectCount, uint32_t objectSize)
{
    try {
        client->client->testingFill(tableId, key, keyLength,
                                    objectCount, objectSize);
    } catch (const ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

Status
rc_set_runtime_option(struct rc_client* client,
                              const char* option,
                              const char* value)
{
    try {
        client->client->setRuntimeOption(option, value);
    } catch (const ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

void
rc_testing_wait_for_all_tablets_normal(struct rc_client* client,
                                       uint64_t timeoutNs)
{
    client->client->testingWaitForAllTabletsNormal(timeoutNs);
}

void
rc_set_log_file(const char* path)
{
    Logger::get().setLogFile(path);
}

