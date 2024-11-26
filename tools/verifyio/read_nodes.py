#!/usr/bin/env python
# encoding, utf-8
import struct
from itertools import repeat
from verifyio_graph import VerifyIONode

accepted_mpi_funcs = [
 'MPI_Send', 'MPI_Ssend', 'MPI_Issend', 'MPI_Isend',
 'MPI_Recv', 'MPI_Sendrecv', 'MPI_Irecv',
 'MPI_Wait', 'MPI_Waitall', 'MPI_Waitany',
 'MPI_Waitsome', 'MPI_Test', 'MPI_Testall',
 'MPI_Testany', 'MPI_Testsome', 'MPI_Bcast',
 'MPI_Ibcast', 'MPI_Reduce', 'MPI_Ireduce',
 'MPI_Gather', 'MPI_Igather', 'MPI_Gatherv',
 'MPI_Igatherv', 'MPI_Barrier', 'MPI_Alltoall',
 'MPI_Allreduce', 'MPI_Allgatherv', 
 'MPI_Reduce_scatter', 'MPI_File_open',
 'MPI_File_close', 'MPI_File_read_at_all',
 'MPI_File_write_at_all', 'MPI_File_set_size',
 'MPI_File_set_view', 'MPI_File_sync', 
 'MPI_File_read_all', 'MPI_File_read_ordered',
 'MPI_File_write_all','MPI_File_write_ordered',
 'MPI_Comm_dup', 'MPI_Comm_split',
 'MPI_Comm_split_type', 'MPI_Cart_create',
 'MPI_Cart_sub'
]

accepted_meta_funcs = [
 'fsync', 'open', 'fopen', 'close', 'fclose'
]

def create_verifyio_node(rank_seqid_tuple, reader):
    rank, seq_id = rank_seqid_tuple
    func = reader.funcs[reader.records[rank][seq_id].func_id]
    return VerifyIONode(rank, seq_id, func)


def read_verifyio_nodes_and_conflicts(reader):

    vio_nodes = [[] for i in repeat(None, reader.nprocs)]

    func_list = reader.funcs
    for rank in range(reader.nprocs):
        records = reader.records[rank]
        for seq_id in range(reader.num_records[rank]):
            func = func_list[records[seq_id].func_id]
            mpifh = None
            # Retrive needed MPI calls
            if func in accepted_mpi_funcs:
                if func.startswith("MPI_File"):
                    mpifh = records[seq_id].args[0].decode('utf-8')
                mpi_node = VerifyIONode(rank, seq_id, func, -1, mpifh)
                vio_nodes[rank].append(mpi_node)
            # Retrive needed metadata I/O calls
            elif func in accepted_meta_funcs:
                fh = records[seq_id].args[0].decode('utf-8')
                metadata_io_node = VerifyIONode(rank, seq_id, func, -1, fh)
                vio_nodes[rank].append(metadata_io_node)

    # Finally, retrive needed I/O calls according
    # to the conflict file
    conflict_rank_seqid_groups = read_all_conflicts(reader)
    unique_conflict_ops = {}

    conflict_vio_node_groups = []

    for cg in conflict_rank_seqid_groups:
        # cg[0] is (c1_rank, c1_seq_id)
        # cg[1] is rank grouped list of c2_rank, c2_seq_id)

        if cg[0] not in unique_conflict_ops:
            c1 = create_verifyio_node(cg[0], reader)
            unique_conflict_ops[cg[0]] = c1
            vio_nodes[cg[0][0]].append(c1)
        else:
            c1 = unique_conflict_ops[cg[0]]

        c2s = [[] for _ in range(reader.nprocs)]
        for c2_rank, c2s_per_rank in enumerate(cg[1]):
            for c2_seq_id in c2s_per_rank:
                c2_rank_seqid = (c2_rank, c2_seq_id)
                if (c2_rank, c2_seq_id) not in unique_conflict_ops:
                    c2 = create_verifyio_node(c2_rank_seqid, reader)
                    unique_conflict_ops[c2_rank_seqid] = c2
                    vio_nodes[c2_rank].append(c2)
                else:
                    c2 = unique_conflict_ops[c2_rank_seqid]
                c2s[rank].append(c2)

        group = [c1, c2s]
        conflict_vio_node_groups.append(group)

    return vio_nodes, conflict_vio_node_groups


def read_one_conflict_group(f, reader):
    data = f.read(16)
    if not data:
        return None

    conflict_ops = [[] for _ in range(reader.nprocs)]

    # int, int, size_t
    c1_rank, c1_seqid, num_pairs = struct.unpack("iiN", data)
    #print(c1_rank, c1_seqid, num_pairs)
    offset = 0
    data = f.read(4*num_pairs*2)
    for i in range(num_pairs):
        c2_rank, c2_seqid = struct.unpack("ii", data[offset:offset+8])
        offset += 8
        conflict_ops[c2_rank].append(c2_seqid)

    conflict_group = ((c1_rank, c1_seqid), conflict_ops)
    return conflict_group


'''
Read conflict pairs from the conflict file generated
using the conflict-detector program.

The conflict file is a binary file has the following format:

for each conflict group:
    c1_rank:int, c1_seq_id:int, num_pairs:size_t
    c2_rank:int, c2_seq_id:int, c2_rank:int, c2_seq_id:int, ...

This function returns a list of conflict groups, it will then
be used later to create groups of VerifyIONode and return to
the caller.
Each conflict group has this format:
[(c1_rank,c1_seq_id), [/*rank0:*/[seqid1, seqid2,...], /*rank1*/:[...] ]]

'''
def read_all_conflicts(reader):
    conflict_groups = []
    with open(reader.logs_dir+"/conflicts.dat", mode="rb") as f:
        while True:
            conflict_group = read_one_conflict_group(f, reader)
            if conflict_group:
                conflict_groups.append(conflict_group)
            else:
                # reached the end of file
                break
    return conflict_groups


if __name__ == "__main__":
    read_all_conflict_pairs()
