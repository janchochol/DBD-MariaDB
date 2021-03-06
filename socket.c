/*
 * On WIN32 windows.h and winsock.h need to be included before mysql.h
 * Otherwise SOCKET type which is needed for mysql.h is not defined
 */
#ifdef _WIN32
#include <windows.h>
#include <winsock.h>
#endif
#include <mysql.h>
#include <stddef.h>
#include <errno.h>

#ifndef _WIN32
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#endif

/*
 * Warning: Native socket code must be outside of dbdimp.c and dbdimp.h because
 *          perl header files redefine socket function. This file must not
 *          include any perl header files!
 */

int mariadb_dr_socket_ready(my_socket fd)
{
  struct timeval timeout;
  fd_set fds;
  int retval;

  FD_ZERO(&fds);
  FD_SET(fd, &fds);

  timeout.tv_sec = 0;
  timeout.tv_usec = 0;

  retval = select(fd+1, &fds, NULL, NULL, &timeout);
  if (retval < 0) {
#ifdef _WIN32
    /* Windows does not update errno */
    return -WSAGetLastError();
#else
    return -errno;
#endif
  }

  return retval;
}
