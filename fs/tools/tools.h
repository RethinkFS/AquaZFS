//
// Created by chiro on 23-6-3.
//

#ifndef ROCKSDB_TOOLS_H
#define ROCKSDB_TOOLS_H

#include <gflags/gflags.h>

#include "../fs_aquafs.h"
#include "../zbd_aquafs.h"

DECLARE_string(zbd);
DECLARE_string(zonefs);
DECLARE_string(raids);
DECLARE_string(aux_path);
DECLARE_bool(force);
DECLARE_string(path);
DECLARE_int32(finish_threshold);
DECLARE_string(restore_path);
DECLARE_string(backup_path);
DECLARE_string(src_file);
DECLARE_string(dest_file);
DECLARE_bool(enable_gc);

namespace aquafs {

int aquafs_tool_mkfs();
int aquafs_tool_list();
int aquafs_tool_df();
int aquafs_tool_lsuuid();

IOStatus aquafs_tool_copy_file(FileSystem *f_fs,
                               const std::string &f,
                               FileSystem *t_fs,
                               const std::string &t);
IOStatus aquafs_tool_copy_dir(FileSystem *f_fs,
                              const std::string &f_dir,
                              FileSystem *t_fs,
                              const std::string &t_dir);

int aquafs_tool_backup();
int aquafs_tool_link();
int aquafs_tool_delete_file();
int aquafs_tool_rename_file();
int aquafs_tool_remove_directory();
int aquafs_tool_restore();
int aquafs_tool_dump();
int aquafs_tool_fsinfo();

int aquafs_tools(int argc, char **argv);
long aquafs_tools_call(const std::vector<std::string> &v);

void prepare_test_env(int num = 4);
// void prepare_test_env();

std::unique_ptr<ZonedBlockDevice> zbd_open(bool readonly, bool exclusive);
Status aquafs_mount(std::unique_ptr<ZonedBlockDevice> &zbd,
                    std::unique_ptr<AquaFS> *aquaFS, bool readonly);

size_t get_file_hash(std::filesystem::path file);

}  // namespace aquafs

#endif  // ROCKSDB_TOOLS_H
