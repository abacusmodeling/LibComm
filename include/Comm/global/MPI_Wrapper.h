// ===================
//  Author: Peize Lin
//  date: 2022.06.02
// ===================

#pragma once

#include <mpi.h>
#include <stdexcept>
#include <string>
#include <vector>

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



	// MPI_Type_Contiguous_Pool(ie) = MPI_Type_contiguous(2^ie, Type_Base);
	template<MPI_Datatype Type_Base>
	class MPI_Type_Contiguous_Pool
	{
	  public:
		MPI_Datatype operator()(const std::size_t exponent)
		{
			if(type_pool.size()<exponent+1)
			{
				const std::size_t size_old = type_pool.size();
				type_pool.resize(exponent+1);
				for(int ie=size_old; ie<type_pool.size(); ++ie)
				{
					if(!ie)
					{
						type_pool[ie] = Type_Base;
					}
					else
					{
						MPI_Type_contiguous( 1<<ie, Type_Base, &type_pool[ie] );
						MPI_Type_commit( &type_pool[ie] );
					}
				}
			}
			return type_pool[exponent];
		}
		~MPI_Type_Contiguous_Pool()
		{
			for(int ie=1; ie<type_pool.size(); ++ie)
				MPI_Type_free( &type_pool[ie] );
		}
		std::vector<MPI_Datatype> type_pool;
	};
	static MPI_Type_Contiguous_Pool<MPI_CHAR> char_contiguous;
}

}

#undef MPI_CHECK
