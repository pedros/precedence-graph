#!/usr/bin/env python
import re
import os
import sys
import json
import functools

import dag
import networkx


def parse(workflow):
    """Load a workflow json string and return a triplet (name, inputs[],
    outputs[]).
    """

    dat = json.loads(workflow)
    name = dat['name']
    inputs = dat['declaredLineage']['inputs'] + dat['dataLineage']['inputs']
    outputs = dat['declaredLineage']['outputs'] + dat['dataLineage']['outputs']

    return name, inputs, outputs


def compose(*funcs):
    """Compose arbitrary number of one-argument functions."""
    return lambda x: functools.reduce(lambda acc, f: f(acc), funcs, x)


PATTERNS = [r'^hdfs:\/\/nameservice1',
            r'/[A-Z]+$',
            r'(\w+=)?\$\{(YEAR|MONTH|DAY|HOUR|MINUTE)\}\W?',
            r'(\w+=)?\$\{YEAR\}\W\$\{MONTH\}\W\$\{DAY\}',
            r'\d\d\d\d\W\d\d\W\d\d',
            r'(\w+=)?\d+\W?']

PATTERN = re.compile('({0})'.format('|'.join(PATTERNS)))

_clean = compose(
    str.strip,
    lambda x: re.sub(PATTERN, '', x),
    os.path.normcase,
    os.path.normpath,
)

def clean(workflow):
    """Normalize and return a triplet (name, inputs[], outputs[]) for
    downstream matching.
    """

    name, inputs, outputs = workflow
    inputs = [_clean(_clean(i)) for i in inputs]
    outputs = [_clean(_clean(o)) for o in outputs]

    return name, inputs, outputs


if __name__ == '__main__':
    G = dag.from_lineage(clean(parse(line)) for line in sys.stdin)
    networkx.write_edgelist(G, "/tmp/123")
