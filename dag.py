# -*- mode: python; -*-
import networkx
import numpy
import itertools


def random_dag(n, p=0.5, weakly_connected=True):
    """Return a random directed acyclic graph (DAG) with a n nodes and
    p(edges)."""

    adj_matrix = numpy.tril(numpy.random.binomial(1, p, size=(n, n)), -1)
    G = networkx.from_numpy_array(adj_matrix, create_using=PrecedenceGraph())

    if weakly_connected and not networkx.is_weakly_connected(G):
        for g in networkx.weakly_connected_component_subgraphs(G.copy()):
            nodes = list(networkx.topological_sort(g))
            G.add_edge('start', nodes[0])
            G.add_edge(nodes[-1], 'stop')

    assert(networkx.is_directed_acyclic_graph(G))

    return G


def _window(seq, n=2):
    "Return a sliding window from seq with size n."

    it = iter(seq)
    result = tuple(itertools.islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


class PrecedenceGraph(networkx.DiGraph):

    def bernstein(self, a, b):
        """Check whether Bernstein's conditions for parallelizability of
        pairwise sequential actions hold for nodes a and b.

        The conditions are:

        out(a) ∩ in(b)  = ∅
        in(a)  ∩ out(b) = ∅
        out(a) ∩ out(b) = ∅

        Violation of the first condition introduces a flow dependency,
        corresponding to the first segment producing a result used by
        the second segment.

        The second condition represents an anti-dependency, when the
        second segment produces a variable needed by the first
        segment.

        The third and final condition represents an output dependency:
        when two segments write to the same location, the result comes
        from the logically last executed segment.

        Bernstein, Arthur J. "Analysis of programs for parallel
        processing."  IEEE Transactions on Electronic Computers 5
        (1966): 757-763.
        """
        if a not in self or b not in self:
            raise networkx.NetworkXUnfeasible('node(s) not in graph')

        # "Statements", in Bernstein's parlance, actually mean edges,
        # in the sense that s1: a = x + y maps inputs (x, y) to output
        # a. Unioning the node itself to the set of its predecessors
        # and-or successors achieves the same effect. I think.
        a_in = set(self.predecessors(a)).union([a])
        a_out = set(self.successors(a)).union([a])
        b_in = set(self.predecessors(b)).union([b])
        b_out = set(self.successors(b)).union([b])

        return ((a_out.intersection(b_in) == set()) and
                (a_in.intersection(b_out) == set()))
               # skip the third condition (shared outputs)
               # and (a_out.intersection(b_out) == set()) )



    def clustering(self):
        clusters = []

        for (u, v) in _window(networkx.topological_sort(self), 2):

            if not self.bernstein(u, v):
                if not clusters or u not in clusters[-1]:
                    clusters.append([u])
                else:
                    clusters.append([v])
            else:
                if clusters and u in clusters[-1]:
                    clusters[-1].append(v)
                else:
                    clusters.append([u, v])

        # Clusters are no worse than initial precedence graph itself
        assert(len(clusters) <= len(self))

        # Unscheduled tasks found. This should not happen.
        assert(len([n for group in clusters for n in group]) == len(self))

        return clusters


import unittest

class PrecedenceGraphTest(unittest.TestCase):
    test_case = (
        '''
        s1 s2 s3
        s2 s4
        s3 s4
        ''',
        [['s1'], ['s2', 's3'], ['s4']])

        # '''
        # s1 s2 s3 s4
        # s2 s5 s6
        # s3 s5 s6
        # s4 s6
        # s5 s7
        # s6 s7
        # ''': [['s1'], ['s2', 's3', 's4'],  ['s7']]

    def test_clustering(self):
        adjlist = self.test_case[0].strip().split('\n')
        g = networkx.parse_adjlist(adjlist,
                                   create_using=PrecedenceGraph())

        for a, b in zip(self.test_case[1], g.clustering()):
            self.assertSetEqual(set(a), set(b))


if __name__ == '__main__':
    unittest.main()
    # g = random_dag(10, 0.5)
    # print(networkx.dag_longest_path(g))
    # print(g.clustering())
