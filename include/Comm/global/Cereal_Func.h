// ===================
//  Author: Peize Lin
//  date: 2022.05.01
// ===================

#pragma once

#include <mpi.h>
#include <sstream>

namespace Comm
{

namespace Cereal_Func
{
	template<typename... Ts>
	extern void mpi_send(const int rank_recv, const int tag, const MPI_Comm &mpi_comm,
		const Ts&... data);

	template<typename... Ts>
	extern void mpi_isend(const int rank_recv, const int tag, const MPI_Comm &mpi_comm,
		std::string &str, MPI_Request &request,
		const Ts&... data);

	template<typename... Ts>
	extern MPI_Status mpi_recv(const MPI_Comm &mpi_comm,
		Ts&... data);	
}

}

#include "Cereal_Func.hpp"