//=======================
// AUTHOR : Peize Lin
// DATE :   2022-01-05
//=======================

#pragma once

#include "Comm_Trans.h"

#include <vector>
#include <string>
#include <stdexcept>

#include <cereal/archives/binary.hpp>
#include <cereal/types/tuple.hpp>
#include <cereal/types/map.hpp>

#define MPI_CHECK(x) if((x)!=MPI_SUCCESS)	throw std::runtime_error(std::string(__FILE__)+" line "+std::to_string(__LINE__));


template<typename Tkey, typename Tvalue, typename Tdatas_isend, typename Tdatas_recv>
Comm_Trans<Tkey,Tvalue,Tdatas_isend,Tdatas_recv>::Comm_Trans(const MPI_Comm &mpi_comm_in)
	:mpi_comm(mpi_comm_in)
{
	MPI_CHECK (MPI_Comm_size (this->mpi_comm, &this->comm_size));
	MPI_CHECK (MPI_Comm_rank (this->mpi_comm, &this->rank_mine));

	this->set_value_recv
		= [](Tkey &&key, Tvalue &&value, Tdatas_recv &datas_recv)
		{ throw std::logic_error("Function set_value not set."); };
	this->traverse_isend
		= [](const Tdatas_isend &datas_isend, const int rank_isend, std::function<void(const Tkey&, const Tvalue&)> &func)
		{ throw std::logic_error("Function traverse not set."); };
	this->init_datas_local
		= [](const int rank_recv) -> Tdatas_recv
		{ throw std::logic_error("Function init_datas_local not set."); };
	this->add_datas
		= [](Tdatas_recv &&datas_local, Tdatas_recv &datas_recv)
		{ throw std::logic_error("Function add_datas not set."); };
}

/*
template<typename Tkey, typename Tvalue, typename Tdatas_isend, typename Tdatas_recv>
Comm_Trans<Tkey,Tvalue,Tdatas_isend,Tdatas_recv>::Comm_Trans(const Comm_Trans &com)
	:mpi_comm(com.mpi_comm)
{
	//ofs<<"C"<<" ";
	MPI_CHECK (MPI_Comm_size (this->mpi_comm, &this->comm_size));
	MPI_CHECK (MPI_Comm_rank (this->mpi_comm, &this->rank_mine));
	this->set_value_recv = com.set_value_recv;
	this->traverse_isend = com.traverse_isend;
	this->flag_lock_set_value = com.flag_lock_set_value;
	this->init_datas_local = com.init_datas_local;
	this->add_datas = com.add_datas;
}
*/

template<typename Tkey, typename Tvalue, typename Tdatas_isend, typename Tdatas_recv>
void Comm_Trans<Tkey,Tvalue,Tdatas_isend,Tdatas_recv>::communicate(
	const Tdatas_isend &datas_isend,
	Tdatas_recv &datas_recv)
{
//std::ofstream //ofs("master_"+std::to_string(this->rank_mine)+"_"+TO_STRING(std::this_thread::get_id()));
//ofs<<__FILE__<<__LINE__<<std::endl;

	// initialization
	int rank_isend_tmp = 0;
	int rank_recv_working = -1;
	int tag_recv_working = -1;
//ofs<<__FILE__<<__LINE__<<std::endl;

	std::vector<MPI_Request> requests_isend(comm_size);
	std::vector<std::stringstream> sss_isend(comm_size);
	std::vector<std::future<void>> futures_isend(comm_size);
	std::vector<std::future<void>> futures_recv(comm_size);
	std::atomic_flag lock_set_value = ATOMIC_FLAG_INIT;
//ofs<<__FILE__<<__LINE__<<std::endl;

	std::future<void> future_post_process = std::async (std::launch::async,
		std::bind( &Comm_Trans::post_process, *this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4 ),
		std::ref(requests_isend), std::ref(sss_isend), std::ref(futures_isend), std::ref(futures_recv));
//ofs<<__FILE__<<__LINE__<<std::endl;

	while (future_post_process.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
	{
		if (rank_isend_tmp < this->comm_size)		// && enough_memory())
		{
			const int rank_isend = (rank_isend_tmp + this->rank_mine) % this->comm_size;
//ofs<<__FILE__<<__LINE__<<"\t"<<rank_isend_tmp<<"\t"<<rank_isend<<std::endl;
			futures_isend[rank_isend] = std::async (std::launch::async,
				std::bind( &Comm_Trans::isend_data, *this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4 ),
				rank_isend, std::cref(datas_isend), std::ref(sss_isend[rank_isend]), std::ref(requests_isend[rank_isend]));
			++rank_isend_tmp;
//ofs<<__FILE__<<__LINE__<<std::endl;
		}

		int flag_iprobe=0;
		MPI_Status status_recv;
		MPI_CHECK (MPI_Iprobe (MPI_ANY_SOURCE, MPI_ANY_TAG, this->mpi_comm, &flag_iprobe, &status_recv));
		if (flag_iprobe
			&& (rank_recv_working!=status_recv.MPI_SOURCE || tag_recv_working!=status_recv.MPI_TAG))
		{
//ofs<<__FILE__<<__LINE__<<"\t"<<status_recv.MPI_SOURCE<<std::endl;
			if (status_recv.MPI_TAG==Comm_Trans::tag_data)
			{
				futures_recv[status_recv.MPI_SOURCE] = std::async (std::launch::async,
					std::bind( &Comm_Trans::recv_data, *this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3 ),
					std::ref(datas_recv), std::cref(status_recv), std::ref(lock_set_value));
			}
			else
				throw std::invalid_argument("MPI_Iprobe from rank "+std::to_string(status_recv.MPI_TAG)+" tag "+std::to_string(status_recv.MPI_SOURCE));
			rank_recv_working = status_recv.MPI_SOURCE;
			tag_recv_working  = status_recv.MPI_TAG;
//ofs<<__FILE__<<__LINE__<<std::endl;
		}
//ofs<<__FILE__<<__LINE__<<std::endl;
	}
//ofs<<__FILE__<<__LINE__<<std::endl;
	future_post_process.get();
//ofs<<__FILE__<<__LINE__<<std::endl;
}


template<typename Tkey, typename Tvalue, typename Tdatas_isend, typename Tdatas_recv>
void Comm_Trans<Tkey,Tvalue,Tdatas_isend,Tdatas_recv>::isend_data(
	const int rank_isend,
	const Tdatas_isend &datas_isend,
	std::stringstream &ss_isend,
	MPI_Request &request_isend) const
{
//std::ofstream //ofs("isend_"+std::to_string(this->rank_mine)+"_"+TO_STRING(std::this_thread::get_id()));
	{
//ofs<<__FILE__<<__LINE__<<"\t"<<rank_isend<<std::endl;
		cereal::BinaryOutputArchive oar(ss_isend);
//ofs<<__FILE__<<__LINE__<<std::endl;

		size_t size_item = 0;
		oar(size_item);					// 占位
//ofs<<__FILE__<<__LINE__<<std::endl;

		std::function<void(const Tkey&, const Tvalue&)> archive_data = [&oar, &size_item](
			const Tkey &key, const Tvalue &value)
		{
			oar(key, value);
			++size_item;
		};
//ofs<<__FILE__<<__LINE__<<std::endl;
		this->traverse_isend(datas_isend, rank_isend, archive_data);
//ofs<<__FILE__<<__LINE__<<"\t"<<size_item<<std::endl;

		ss_isend.rdbuf()->pubseekpos(0);		// 返回size_item的占位，序列化真正的size_item值
		oar(size_item);
//ofs<<__FILE__<<__LINE__<<std::endl;
	} // end cereal::BinaryOutputArchive
//ofs<<__FILE__<<__LINE__<<std::endl;
	MPI_CHECK (MPI_Isend (ss_isend.str().c_str(), ss_isend.str().size(), MPI_CHAR, rank_isend, Comm_Trans::tag_data, this->mpi_comm, &request_isend));
//ofs<<__FILE__<<__LINE__<<std::endl;
}


template<typename Tkey, typename Tvalue, typename Tdatas_isend, typename Tdatas_recv>
void Comm_Trans<Tkey,Tvalue,Tdatas_isend,Tdatas_recv>::recv_data (
	Tdatas_recv &datas_recv,
	const MPI_Status &status_recv,
	std::atomic_flag &lock_set_value)
{
//std::ofstream //ofs("recv_"+std::to_string(this->rank_mine)+"_"+TO_STRING(std::this_thread::get_id()));
//ofs<<__FILE__<<__LINE__<<"\t"<<status_recv.MPI_SOURCE<<std::endl;
	int size_mpi;	MPI_CHECK (MPI_Get_count (&status_recv, MPI_CHAR, &size_mpi));
	std::vector<char> buffer_recv(size_mpi);                                                             
	MPI_CHECK (MPI_Recv (buffer_recv.data(), size_mpi, MPI_CHAR, status_recv.MPI_SOURCE, status_recv.MPI_TAG, this->mpi_comm, MPI_STATUS_IGNORE));     
//ofs<<__FILE__<<__LINE__<<std::endl;

	std::stringstream ss_recv;  
	ss_recv.rdbuf()->pubsetbuf(buffer_recv.data(), size_mpi);
	{
//ofs<<__FILE__<<__LINE__<<std::endl;
		cereal::BinaryInputArchive iar(ss_recv);
		size_t size_item;	iar(size_item);
//ofs<<__FILE__<<__LINE__<<"\t"<<size_item<<std::endl;

		if (this->flag_lock_set_value==Comm_Tools::Lock_Type::Lock_free)
		{
//ofs<<__FILE__<<__LINE__<<std::endl;
			for (size_t i=0; i<size_item; ++i)
			{
//ofs<<__FILE__<<__LINE__<<"\t"<<i<<std::endl;
				Tkey key;
				Tvalue value;
				iar(key, value);
//ofs<<__FILE__<<__LINE__<<std::endl;

				this->set_value_recv(std::move(key), std::move(value), datas_recv);
//ofs<<__FILE__<<__LINE__<<std::endl;
			}
//ofs<<__FILE__<<__LINE__<<std::endl;
		}
		else if (this->flag_lock_set_value==Comm_Tools::Lock_Type::Lock_item)
		{
			for (size_t i=0; i<size_item; ++i)
			{
				Tkey key;
				Tvalue value;
				iar(key, value);

				while (lock_set_value.test_and_set(std::memory_order_seq_cst)) std::this_thread::yield();
				this->set_value_recv(std::move(key), std::move(value), datas_recv);
				lock_set_value.clear(std::memory_order_seq_cst);
			}
		}
		else if (this->flag_lock_set_value==Comm_Tools::Lock_Type::Lock_Process)
		{
			while (lock_set_value.test_and_set(std::memory_order_seq_cst)) std::this_thread::yield();
			for (size_t i=0; i<size_item; ++i)
			{
				Tkey key;
				Tvalue value;
				iar(key, value);

				this->set_value_recv(std::move(key), std::move(value), datas_recv);
			}
			lock_set_value.clear(std::memory_order_seq_cst);
		}
		else if (this->flag_lock_set_value==Comm_Tools::Lock_Type::Copy_merge)
		{
			Tdatas_recv datas_local = this->init_datas_local (status_recv.MPI_SOURCE);
			for (size_t i=0; i<size_item; ++i)
			{
				Tkey key;
				Tvalue value;
				iar(key, value);

				this->set_value_recv (std::move(key), std::move(value), datas_local);
			}
			while (lock_set_value.test_and_set(std::memory_order_seq_cst)) std::this_thread::yield();
			this->add_datas (std::move(datas_local), datas_recv);
			lock_set_value.clear(std::memory_order_seq_cst);
		}
		else
		{
			throw std::invalid_argument(
				+" file "+std::string(__FILE__)
				+" line "+std::to_string(__LINE__)
				+" rank_mine "+std::to_string(this->rank_mine)
				+" rank_recv "+std::to_string(status_recv.MPI_SOURCE));
		}
//ofs<<__FILE__<<__LINE__<<std::endl;
	} // end cereal::BinaryInputArchive
//ofs<<__FILE__<<__LINE__<<std::endl;
}


template<typename Tkey, typename Tvalue, typename Tdatas_isend, typename Tdatas_recv>
void Comm_Trans<Tkey,Tvalue,Tdatas_isend,Tdatas_recv>::post_process(
	std::vector<MPI_Request> &requests_isend,
	std::vector<std::stringstream> &sss_isend,
	std::vector<std::future<void>> &futures_isend,
	std::vector<std::future<void>> &futures_recv) const
{
//std::ofstream //ofs("post_"+std::to_string(this->rank_mine)+"_"+TO_STRING(std::this_thread::get_id()));
//ofs<<__FILE__<<__LINE__<<std::endl;
	int rank_isend_free_tmp = 0;
	int rank_recv_free_tmp = 0;
//ofs<<__FILE__<<__LINE__<<std::endl;
	while (rank_isend_free_tmp < this->comm_size
		|| rank_recv_free_tmp < this->comm_size)
	{
		while (rank_isend_free_tmp < this->comm_size)
		{
			const int rank_isend_free = (this->rank_mine+rank_isend_free_tmp)%this->comm_size;
			if (futures_isend[rank_isend_free].valid() 
				&& futures_isend[rank_isend_free].wait_for(std::chrono::seconds(0)) == std::future_status::ready)
			{
				int flag_finish;
				MPI_CHECK (MPI_Test (&(requests_isend[rank_isend_free]), &flag_finish, MPI_STATUS_IGNORE));
				if (flag_finish)
				{
//ofs<<__FILE__<<__LINE__<<"\t"<<rank_isend_free<<std::endl;
	//				MPI_CHECK (MPI_Request_free (&requests_isend[rank_isend_free]));
					futures_isend[rank_isend_free].get();
					sss_isend[rank_isend_free] = std::stringstream();
					++rank_isend_free_tmp;
				}
				else{ break; }
			}
			else{ break; }
		}

		while (rank_recv_free_tmp < this->comm_size)
		{
			const int rank_recv_free = (this->rank_mine+rank_recv_free_tmp)%this->comm_size;
			if (futures_recv[rank_recv_free].valid()
				&& futures_recv[rank_recv_free].wait_for(std::chrono::seconds(0)) == std::future_status::ready)
			{
//ofs<<__FILE__<<__LINE__<<"\t"<<rank_recv_free<<std::endl;
				futures_recv[rank_recv_free].get();
				++rank_recv_free_tmp;
			}
			else{ break; }
		}
	
		std::this_thread::yield();
	}
}

#undef MPI_CHECK

/*
get_send_keys()
{
	
	if(unique)
	{
		for(irank in all)
			send(irank_send, atom_pairs_remove);
	}
}
*/