// ===================
//  Author: Peize Lin
//  date: 2022.05.01
// ===================

#pragma once

#include <mpi.h>
#include <sstream>
#include <vector>

namespace Comm
{

namespace Cereal_Func
{
	// Send str
	extern inline void mpi_send(const std::string &str, const std::size_t exponent_align, const int rank_recv, const int tag, const MPI_Comm &mpi_comm);

	// Send data
	template<typename... Ts>
	extern void mpi_send(const int rank_recv, const int tag, const MPI_Comm &mpi_comm,
		const Ts&... data);

	// Isend str
	extern inline void mpi_isend(const std::string &str, const std::size_t exponent_align, const int rank_recv, const int tag, const MPI_Comm &mpi_comm, MPI_Request &request);

	// Isend data using temporary memory str
	template<typename... Ts>
	extern void mpi_isend(const int rank_recv, const int tag, const MPI_Comm &mpi_comm,
		std::string &str, MPI_Request &request,
		const Ts&... data);

	// Recv to data
	template<typename... Ts>
	extern MPI_Status mpi_recv(const MPI_Comm &mpi_comm,
		Ts&... data);

	// Mrecv to return
	extern inline std::vector<char> mpi_mrecv(MPI_Message &message_recv, const MPI_Status &status);

	// every 2^exponent_align char concatenate to 1 word
	extern inline std::size_t align_stringstream(std::stringstream &ss);
}

}

#include "Cereal_Func.hpp"