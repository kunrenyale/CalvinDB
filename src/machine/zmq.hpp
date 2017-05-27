/*
    Copyright (c) 2007-2011 iMatix Corporation
    Copyright (c) 2007-2011 Other contributors as noted in the AUTHORS file

    This file is part of 0MQ.

    0MQ is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    0MQ is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
// Modified: Kun Ren <renkun.nwpu@gmail.com>

#ifndef __ZMQ_HPP_INCLUDED__
#define __ZMQ_HPP_INCLUDED__

#include "zmq.h"

#include <cassert>
#include <cstring>
#include <exception>

//  Detect whether the compiler supports C++11 rvalue references.
#if (defined(__GNUC__) && (__GNUC__ > 4 || \
      (__GNUC__ == 4 && __GNUC_MINOR__ > 2)) && \
      defined(__GXX_EXPERIMENTAL_CXX0X__))
    #define ZMQ_HAS_RVALUE_REFS
    #define ZMQ_DELETED_FUNCTION = delete
#elif defined(__clang__)
    #if __has_feature(cxx_rvalue_references)
        #define ZMQ_HAS_RVALUE_REFS
    #endif

    #if __has_feature(cxx_deleted_functions)
        #define ZMQ_DELETED_FUNCTION = delete
    #endif
#elif defined(_MSC_VER) && (_MSC_VER >= 1600)
    #define ZMQ_HAS_RVALUE_REFS
    #define ZMQ_DELETED_FUNCTION
#else
    #define ZMQ_DELETED_FUNCTION
#endif

// In order to prevent unused variable warnings when building in non-debug
// mode use this macro to make assertions.
#ifndef NDEBUG
#   define ZMQ_ASSERT(expression) assert(expression)
#else
#   define ZMQ_ASSERT(expression) (expression)
#endif


namespace zmq
{

    typedef zmq_free_fn free_fn;
    typedef zmq_pollitem_t pollitem_t;

    class error_t : public std::exception
    {
    public:

        error_t () : errnum (zmq_errno ()) {}

        virtual const char *what () const throw ()
        {
            return zmq_strerror (errnum);
        }

        int num () const
        {
            return errnum;
        }

    private:

        int errnum;
    };

    inline int poll (zmq_pollitem_t *items_, int nitems_, long timeout_ = -1)
    {
        int rc = zmq_poll (items_, nitems_, timeout_);
        return rc;
    }

    inline void version (int *major_, int *minor_, int *patch_)
    {
        zmq_version (major_, minor_, patch_);
    }

    class message_t
    {
        friend class socket_t;

    public:

        inline message_t ()
        {
            zmq_msg_init (&msg);
        }

        inline explicit message_t (size_t size_)
        {
            zmq_msg_init_size (&msg, size_);
        }

        inline message_t (void *data_, size_t size_, free_fn *ffn_,
            void *hint_ = NULL)
        {
            zmq_msg_init_data (&msg, data_, size_, ffn_, hint_);
        }

#ifdef ZMQ_HAS_RVALUE_REFS
        inline message_t (message_t &&rhs) : msg (rhs.msg)
        {
            zmq_msg_init (&rhs.msg);
        }

        inline message_t &operator = (message_t &&rhs)
        {
            std::swap (msg, rhs.msg);
            return *this;
        }
#endif

        inline ~message_t ()
        {
            int rc = zmq_msg_close (&msg);
            ZMQ_ASSERT (rc == 0);
        }

        inline void rebuild ()
        {
            zmq_msg_close (&msg);
            zmq_msg_init (&msg);
        }

        inline void rebuild (size_t size_)
        {
            zmq_msg_close (&msg);
            zmq_msg_init_size (&msg, size_);
        }

        inline void rebuild (void *data_, size_t size_, free_fn *ffn_,
            void *hint_ = NULL)
        {
            zmq_msg_close (&msg);
            zmq_msg_init_data (&msg, data_, size_, ffn_, hint_);
        }

        inline void move (message_t *msg_)
        {
            zmq_msg_move (&msg, &(msg_->msg));
        }

        inline void copy (message_t *msg_)
        {
            zmq_msg_copy (&msg, &(msg_->msg));
        }

        inline void *data ()
        {
            return zmq_msg_data (&msg);
        }

        inline const void* data () const
        {
            return zmq_msg_data (const_cast<zmq_msg_t*>(&msg));
        }

        inline size_t size () const
        {
            return zmq_msg_size (const_cast<zmq_msg_t*>(&msg));
        }

    private:

        //  The underlying message
        zmq_msg_t msg;

        //  Disable implicit message copying, so that users won't use shared
        //  messages (less efficient) without being aware of the fact.
        message_t (const message_t&);
        void operator = (const message_t&);
    };

    class context_t
    {
        friend class socket_t;

    public:

        inline explicit context_t (int io_threads_)
        {
            ptr = zmq_init (io_threads_);
        }

#ifdef ZMQ_HAS_RVALUE_REFS
        inline context_t (context_t &&rhs) : ptr (rhs.ptr)
        {
            rhs.ptr = NULL;
        }
        inline context_t &operator = (context_t &&rhs)
        {
            std::swap (ptr, rhs.ptr);
            return *this;
        }
#endif

        inline ~context_t ()
        {
            close();
        }

        inline void close()
        {
            if (ptr == NULL)
                return;
            int rc = zmq_term (ptr);
            ZMQ_ASSERT (rc == 0);
            ptr = NULL;
        }

        //  Be careful with this, it's probably only useful for
        //  using the C api together with an existing C++ api.
        //  Normally you should never need to use this.
        inline operator void* ()
        {
            return ptr;
        }

    private:

        void *ptr;

        context_t (const context_t&);
        void operator = (const context_t&);
    };

    class socket_t
    {
    public:

        inline socket_t (context_t &context_, int type_)
        {
            ptr = zmq_socket (context_.ptr, type_);
        }

#ifdef ZMQ_HAS_RVALUE_REFS
        inline socket_t(socket_t&& rhs) : ptr(rhs.ptr)
        {
            rhs.ptr = NULL;
        }
        inline socket_t& operator=(socket_t&& rhs)
        {
            std::swap(ptr, rhs.ptr);
            return *this;
        }
#endif

        inline ~socket_t ()
        {
            close();
        }

        inline operator void* ()
        {
            return ptr;
        }

        inline void close()
        {
            if(ptr == NULL)
                // already closed
                return ;
            int rc = zmq_close (ptr);
            ZMQ_ASSERT (rc == 0);
            ptr = 0 ;
        }

        inline void setsockopt (int option_, const void *optval_,
            size_t optvallen_)
        {
            zmq_setsockopt (ptr, option_, optval_, optvallen_);
        }

        inline void getsockopt (int option_, void *optval_,
            size_t *optvallen_)
        {
            zmq_getsockopt (ptr, option_, optval_, optvallen_);
        }

        inline void bind (const char *addr_)
        {
            zmq_bind (ptr, addr_);
        }

        inline void connect (const char *addr_)
        {
            zmq_connect (ptr, addr_);
        }

        inline bool connected()
        {
            return(ptr != NULL);
        }

        inline size_t send (const void *buf_, size_t len_, int flags_ = 0)
        {
            int nbytes = zmq_send (ptr, buf_, len_, flags_);
            if (nbytes >= 0)
                return (size_t) nbytes;
            if (zmq_errno () == EAGAIN)
                return 0;
        }

        inline bool send (message_t &msg_, int flags_ = 0)
        {
            int nbytes = zmq_msg_send (&(msg_.msg), ptr, flags_);
            if (nbytes >= 0)
                return true;
            if (zmq_errno () == EAGAIN)
                return false;
            exit(1);
        }

        inline size_t recv (void *buf_, size_t len_, int flags_ = 0)
        {
            int nbytes = zmq_recv (ptr, buf_, len_, flags_);
            if (nbytes >= 0)
                return (size_t) nbytes;
            if (zmq_errno () == EAGAIN)
                return 0;
            exit(1);
        }

        inline bool recv (message_t *msg_, int flags_ = 0)
        {
            int nbytes = zmq_msg_recv (&(msg_->msg), ptr, flags_);
            if (nbytes >= 0)
                return true;
            if (zmq_errno () == EAGAIN)
                return false;
            exit(1);
        }

    private:

        void *ptr;

        socket_t (const socket_t&) ZMQ_DELETED_FUNCTION;
        void operator = (const socket_t&) ZMQ_DELETED_FUNCTION;
    };

}

#endif

