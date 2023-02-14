// ===================
//  Author: Peize Lin
//  date: 2022.06.02
// ===================

#pragma once

#include <mpi.h>
#include <stdexcept>
#include <string>

#define MPI_CHECK(x) if((x)!=MPI_SUCCESS)	throw std::runtime_error(std::string(__FILE__)+" line "+std::to_string(__LINE__));

namespace Comm
{

namespace MPI_Wrapper
{
	inline int mpi_get_rank(const MPI_Comm &mpi_comm)
	{
		int rank_mine;
		MPI_CHECK( MPI_Comm_rank (mpi_comm, &rank_mine) );
		return rank_mine;
	}

	inline int mpi_get_size(const MPI_Comm &mpi_comm)
	{
		int rank_size;
		MPI_CHECK( MPI_Comm_size (mpi_comm, &rank_size) );
		return rank_size;
	}

#if MPI_VERSION>=4
	inline MPI_Count mpi_get_count(const MPI_Status &status, const MPI_Datatype &datatype)
	{
		MPI_Count count;
		MPI_CHECK( MPI_Get_count_c(&status, datatype, &count) );
		return count;
	}
#else
	inline int mpi_get_count(const MPI_Status &status, const MPI_Datatype &datatype)
	{
		int count;
		MPI_CHECK( MPI_Get_count  (&status, datatype, &count) );
		return count;
	}
#endif
}

}

#undef MPI_CHECK
