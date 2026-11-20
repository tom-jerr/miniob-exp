#pragma once

#include "common/sys/rc.h"

class Db;

// 手动/离线 GC：回收所有活跃快照都不可见的旧版本。
// 没有后台线程；调用方必须在可控的并发窗口内调用。
class MvccGc
{
public:
  RC collect(Db &db);
};
