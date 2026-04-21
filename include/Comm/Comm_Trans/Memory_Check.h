//=======================
// AUTHOR : Peize Lin
// DATE :   2026-04-21
//=======================

#pragma once

#include "../global/Global_Func.h"

#include<atomic>

namespace Comm
{

class Memory_Check
{
  public:
	bool first_check = false;
	std::atomic<bool> first_set = false;
	std::atomic<std::size_t> max_used = 0;

	bool enough()
	{
		if(!this->first_check)
		{
			this->first_check = true;
			return true;
		}
		else if(!this->first_set)
		{
			return false;
		}
		else
		{
			return Global_Func::memory_available() > this->max_used.load() * 2;
		}
	}
};

}