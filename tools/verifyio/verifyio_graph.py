#!/usr/bin/env python
# encoding: utf-8
import networkx as nx

class VerifyIONode:
    def __init__(self, rank, seq_id, func, fd = -1, mpifh = None):
        self.rank = rank
        # This is the index with respect to all
        # Recorder trace records
        self.seq_id = seq_id
        # This is the index with respect to all
        # VerifyIONode we read
        self.index  = -1
        self.func = func
        # An integer maps to the filename
        self.fd = fd
        # Store the MPI-IO file handle so we can match
        # I/O calls with sync/commit calls during
        # the verification.
        self.mpifh = mpifh

    def graph_key(self):
        return str(self.rank) + "-" + str(self.seq_id) + "-" + str(self.func)

    def __str__(self):
        #if "write" in self.func or "read" in self.func or \
        #    self.func.startswith("MPI_File_"):
        #        return "Rank %d: %dth %s(%s)" %(self.rank, self.seq_id, self.func, self.mpifh)
        #else:
        #    return "Rank %d: %dth %s" %(self.rank, self.seq_id, self.func)
        return "<Rank %d: %dth %s>" %(self.rank, self.seq_id, self.func)


'''
Essentially a wrapper for networkx DiGraph
'''
class VerifyIOGraph:
    def __init__(self, nodes, edges, include_vc=False):
        self.G = nx.DiGraph()
        self.nodes = nodes      # A list of VerifyIONode sorted by seq_id
        self.include_vc = include_vc
        self.__build_graph(nodes, edges, include_vc)

    def num_nodes(self):
        return len(self.G.nodes)

    # next (program-order) node of funcs in the sam rank
    def next_po_node(self, current, funcs):
        nodes = self.nodes[current.rank]
        if not funcs:
            return nodes[current.index+1]
        target = None
        for i in range(current.index+1, len(nodes)):
            if nodes[i].func in funcs:
                target = nodes[i]
                break
        return target

    # previous (program-order) node of funcs in the same rank
    def prev_po_node(self, current, funcs):
        nodes = self.nodes[current.rank]
        if not funcs:
            return nodes[current.index-1]
        target = None
        for i in range(current.index-1, 0, -1):
            if nodes[i].func in funcs:
                target = nodes[i]
                break
        return target

    # next (happens-beofre) node of funcs in the target rank
    def next_hb_node(self, current, funcs, target_rank):
        target = None
        nodes = self.nodes[target_rank]
        for target in nodes:
            if (target.func in funcs) and (self.has_path(current, target)):
                target.append(target)
                break
        return target

    def add_edge(self, h, t):
        self.G.add_edge(h.graph_key(), t.graph_key())

    def remove_edge(self, h, t):
        self.G.remove_edge(h.graph_key(), t.graph_key())

    def has_path(self, src, dst):
        return nx.has_path(self.G, src.graph_key(), dst.graph_key())

    def plot_graph(self, fname):
        import matplotlib.pyplot as plt
        nx.draw_networkx(self.G)
        #plt.savefig(fname)
        plt.show()

    def get_vector_clock(self, n):
        return self.G.nodes[n.graph_key()]['vc']

    # caller need to assume there is no cycle
    # in the DAG.
    def run_vector_clock(self):
        for node_key in nx.topological_sort(self.G):
            vc = self.G.nodes[node_key]['vc']
            for eachpred in self.G.predecessors(node_key):
                pred_vc = self.G.nodes[eachpred]['vc'].copy()
                pred_vc[self.key2rank(eachpred)] += 1
                vc = list(map(max, zip(vc, pred_vc)))

            self.G.nodes[node_key]['vc'] = vc
            #print(node_key, vc)

    def run_transitive_closure(self):
        tc = nx.transitive_closure(self.G)

    # Retrive rank from node key
    def key2rank(self, key):
        return (int)(key.split('-')[0])

    def shortest_path(self, src, dst):

        if (not src) or (not dst):
            print("shortest_path Error: must specify src and dst (VerifyIONode)")
            return []

        # nx.shortest_path will return a list of nodes in
        # keys. we then retrive the real VerifyIONode and return a
        # list of them
        path_in_keys = nx.shortest_path(self.G, src.graph_key(), dst.graph_key())
        path = []
        for key in path_in_keys:
            path.append(key)
        return path


    # private method to build the networkx DiGraph
    # called only by __init__
    # nodes: per rank of nodes of type VerifyIONode
    # Add neighbouring nodes of the same rank
    # This step will add all nodes
    def __build_graph(self, all_nodes, mpi_edges, include_vc):
        # 1. Add program orders
        nprocs = len(all_nodes)
        for rank in range(nprocs):
            for i in range(len(all_nodes[rank]) - 1):
                h = all_nodes[rank][i]
                t = all_nodes[rank][i+1]
                self.add_edge(h, t)

                # Include vector clock for each node
                if not include_vc: continue
                vc1 = [0] * (nprocs + 1)    # one more for ghost rank
                vc2 = [0] * (nprocs + 1)    # one more for ghost rank 
                vc1[rank] = i
                vc2[rank] = i+1
                self.G.nodes[h.graph_key()]['vc'] = vc1
                self.G.nodes[t.graph_key()]['vc'] = vc2

            # corner case: when this rank has only one node
            # the code will not run into the previous for loop
            if len(all_nodes[rank]) == 1:
                n = all_nodes[rank][0]
                self.G.add_node(n.graph_key(), vc=[0]*(nprocs+1))

        # 2. Add synchornzation orders (using mpi edges)
        # Before calling this function, we should
        # have added all nodes. We use this function
        # to add edges of matching MPI calls
        from match_mpi import MPICallType
        ghost_node_count = 0
        for edge in mpi_edges:

            # case i: point to point calls
            if edge.call_type == MPICallType.POINT_TO_POINT:
                self.add_edge(edge.head, edge.tail)
                continue

            # case ii: collective calls
            # We handle all collect calls in a same way
            # just use them as a fence to establish order
            # between I/O calls
            # add a ghost node and connect all predecessors
            # and successors from all ranks.
            # This prvents circle
            mpi_calls = edge.get_all_involved_calls()
            if len(mpi_calls) <= 1: continue
            for mpi_call in mpi_calls:

                # Add a ghost node and connect all predecessors
                # and successors from all ranks. This prvents the circle
                ghost_node = VerifyIONode(rank=nprocs, seq_id=ghost_node_count, func="ghost")
                for h in mpi_calls:
                    # use list() to make a copy to avoid the runtime
                    # error of "dictionary size changed during iteration"
                    try:
                        successors = list(self.G.successors(h.graph_key()))
                        for successor in successors:
                            self.G.remove_edge(h.graph_key(), successor)
                        if len(list(self.G.successors(h.graph_key()))) != 0 :
                            print("Not possible!")
                        for successor in successors:
                            self.G.add_edge(ghost_node.graph_key(), successor)
                    except nx.exception.NetworkXError:
                        # when the node has no successors, the G.successors()
                        # function through an error instead of an empty list
                        # (e.g., the last node in the graph)
                        pass

                for h in mpi_calls:
                    self.add_edge(h, ghost_node)

                vc = [0] * (nprocs + 1)
                vc[nprocs] = ghost_node_count
                self.G.nodes[ghost_node.graph_key()]['vc'] = vc
                ghost_node_count += 1

    # Detect cycles of the graph
    # correct code should contain no cycles.
    # incorrect code, e.g., with unmatched collective calls
    # may have cycles
    # e.g., pnetcdf/test/testcases/test-varm.c
    # it uses ncmpi_wait() call, which may result in
    # rank0 calling MPI_File_write_at_all(), and
    # rank1 calling MPI_File_write_all()
    #
    # Our verification algorithm assumes the graph
    # has no code, so we need do this check first.
    def check_cycles(self):
        has_cycles = False
        try:
            cycle = nx.find_cycle(self.G)
            print("Generated graph contain cycles. Original code may have bugs.")

            simplified_cycle = []
            for edge in cycle:
                c1, c2 = edge[0], edge[1]
                rank1 = self.key2rank(c1)
                rank2 = self.key2rank(c2)
                if (rank1 != rank2):
                    simplified_cycle.append(edge)
            print(simplified_cycle)
            has_cycles = True
        except nx.exception.NetworkXNoCycle:
            pass
        return has_cycles

