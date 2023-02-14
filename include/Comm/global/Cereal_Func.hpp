// ===================
//  Author: Peize Lin
//  date: 2022.05.01
// ===================

#pragma once

#include "Cereal_Func.h"
#include "Cereal_Types.h"

#include <cereal/archives/binary.hpp>
#include <sstream>
#include <mpi.h>

#define MPI_CHECK(x) if((x)!=MPI_SUCCESS)	throw std::runtime_error(std::string(__FILE__)+" line "+std::to_string(__LINE__));

namespace Comm
{

namespace Cereal_Func
{
	template<typename... Ts>
	void mpi_send(const int rank_recv, const int tag, const MPI_Comm &mpi_comm,
		const Ts&... data)
	{
		std::stringstream ss;
		{
			cereal::BinaryOutputArchive ar(ss);
			ar(data...);
		}
#if MPI_VERSION>=4
		MPI_CHECK( MPI_Send_c( ss.str().c_str(), ss.str().size(), MPI_CHAR, rank_recv, tag, mpi_comm ) );
#else
		MPI_CHECK( MPI_Send  ( ss.str().c_str(), ss.str().size(), MPI_CHAR, rank_recv, tag, mpi_comm ) );
#endif
	}

	template<typename... Ts>
	void mpi_isend(const int rank_recv, const int tag, const MPI_Comm &mpi_comm,
		std::stringstream &ss, MPI_Request &request,
		const Ts&... data)
	{
		{
			cereal::BinaryOutputArchive ar(ss);
			ar(data...);
		}
#if MPI_VERSION>=4
		MPI_CHECK( MPI_Isend_c( ss.str().c_str(), ss.str().size(), MPI_CHAR, rank_recv, tag, mpi_comm, &request ) );
#else
		MPI_CHECK( MPI_Isend  ( ss.str().c_str(), ss.str().size(), MPI_CHAR, rank_recv, tag, mpi_comm, &request ) );
#endif
	}

	template<typename... Ts>
	MPI_Status mpi_recv(const MPI_Comm &mpi_comm,
		Ts&... data)
	{
		MPI_Status status;
		MPI_CHECK( MPI_Probe( MPI_ANY_SOURCE, MPI_ANY_TAG, mpi_comm, &status ) );

#if MPI_VERSION>=4
		MPI_Count size;		MPI_CHECK( MPI_Get_count_c( &status, MPI_CHAR, &size ) );
		std::vector<char> c(size);
		MPI_CHECK( MPI_Recv_c( c.data(), size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, mpi_comm, MPI_STATUS_IGNORE ) );
#else
		int size;			MPI_CHECK( MPI_Get_count  ( &status, MPI_CHAR, &size ) );
		std::vector<char> c(size);
		MPI_CHECK( MPI_Recv  ( c.data(), size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, mpi_comm, MPI_STATUS_IGNORE ) );
#endif

		std::stringstream ss;
		ss.rdbuf()->pubsetbuf(c.data(), size);
		{
			cereal::BinaryInputArchive ar(ss);
			ar(data...);
		}
		return status;
	}
}

}

#undef MPI_CHECK