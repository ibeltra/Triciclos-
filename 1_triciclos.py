from pyspark import SparkContext
sp = SparkContext()

def get_edges(line): # grafo no dirigido, sin bucles.
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1, n2)
    elif n1 > n2:
         return (n2, n1)
    else:
        pass #n1 == n2

def get_rdd_distict_edges(sp, filename):
    return sp.textFile(filename).\
        map(get_edges).\
        filter(lambda x: x is not None).\
        distinct()

def adjacents(sp, filename):
    nodes = get_rdd_distict_edges(sp, filename)
    adj = nodes.groupByKey().collect()
    print(adj)
    for node in adj:
        print(node[0], list(node[1]))

def auxiliar(tupla):
    result = []
    for i in range(len(tupla[1])):
        result.append(((tupla[0], tupla[1][i]), 'exists'))
        for j in range(i+1, len(tupla[1])):
            result.append(((tupla[1][i], tupla[1][j]), ('pending',tupla[0])))
    return result

def comprobar(e):
    lista = []
    for k in e[1]:
        if k != 'exists':
            lista.append((k[1], e[0][0], e[0][1]))
    return lista

def pendientes(tupla):
    return (len(tupla[1]) >=2 and 'exists' in tupla[1])

def triciclos(sp,filename):
    nodes = get_rdd_distict_edges(sp, filename)
    a = nodes.groupByKey().mapValues(list).flatMap(auxiliar).\
            groupByKey().mapValues(list).filter(pendientes).flatMap(comprobar)
    r = a.collect()
    return r
