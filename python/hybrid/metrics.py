#
# Metrics module
#
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
def nchoosek(n,k):
    """Binomial coefficient"""
    return reduce(lambda a,b: a*(n-b)/(b+1),xrange(k),1)

def validate_cluster_data(c1,c2):
    n1 = len(c1)
    n2 = len(c2)
    if n1 == n2:
        return n1
    else:
        return 0

def rand_index(c1,c2):
    """Computes the Rand index (i.e., agreement) between two data clusterings using contingency table."""
    num_data = validate_cluster_data(c1, c2)
    if num_data:
        N = contingency_table(c1, c2)
        return (N[0][0] + N[1][1])/nchoosek(num_data,2)
    else:
        return -1


def jaccard_index(c1,c2):
    """Computes the Jaccard index (i.e., agreement) between two data clusterings using contingency table."""
    num_data = validate_cluster_data(c1, c2)
    if num_data:
        N = contingency_table(c1, c2)
        denom = N[1][1] + N[0][1] + N[1][0]
        if denom > 0:
            return N[1][1]/denom
        else:
            return 0
    else:
        return -1


def contingency_table(c1,c2):
    """Computes the contingency table from Hubert and Arabie, 1985."""

    # check that data clustering info is consistent
    num_data = validate_cluster_data(c1, c2)

    if num_data:
        c1_labels = list(set(c1))
        c1_ind = {}
        for l in c1_labels: c1_ind[l] = []
        c2_labels = list(set(c2))
        c2_ind = {}
        for l in c2_labels: c2_ind[l] = []
        for i in range(num_data):
            l1 = c1[i]; l2 = c2[i]
            c1_ind[l1].append(i)
            c2_ind[l2].append(i)

        # create contingency table
        cont = []
        for i in range(len(c1_labels)):
            l1 = c1_labels[i]
            cont.append([0]*len(c2_labels))
            for j in range(len(c2_labels)):
                l2 = c2_labels[j]
                cont[i][j] = len(set(c1_ind[l1]) & set(c2_ind[l2]))

        cont_row_sums = []
        for i in range(len(c1_labels)):
            cont_row_sums.append(sum(cont[i]))
        cont_col_sums = []
        for j in range(len(c2_labels)):
            cont_col_sums.append(sum([cont[i][j] for i in range(len(cont))]))

        sum_rowsums_squared = sum([n*n for n in cont_row_sums])
        sum_colsums_squared = sum([n*n for n in cont_col_sums])
        all_cont = sum(cont, [])
        sum_n = sum(all_cont)
        sum_n_squared = sum([n**2 for n in all_cont])

        # pair counts
        N = [[0,0],[0,0]]
        N[0][0] = 0.5*(num_data*num_data + sum_n_squared - (sum_rowsums_squared + sum_colsums_squared))
        N[0][1] = 0.5*(sum_rowsums_squared - sum_n_squared)
        N[1][0] = 0.5*(sum_colsums_squared - sum_n_squared)
        N[1][1] = 0.5*(sum_n_squared - sum_n)

        return N
    else:
        return None


""" storage code:

 def rand_index2(c1,c2):
  '''Computes the Rand index (i.e., agreement) between two data clusterings.'''

  # check that data clustering info is consistent
  num_data = validate_cluster_data(c1, c2)

  if num_data:
    matches = 0.0; # number of data pairs for which both clusterings agree
    for i in range(num_data-1):
      for j in range(i+1,num_data):
        if (c1[i]==c1[j] and c2[i]==c2[j]) or (c1[i]!=c1[j] and c2[i]!=c2[j]):
          matches = matches + 1

    # rand_index = matched pairs divided by total pairs of data
    return matches/nchoosek(num_data,2)
  else:
    return -1

def jaccard_index2(c1,c2):
  '''Computes the Jaccard index between two data clusterings.'''

  # check that data clustering info is consistent
  num_data = validate_cluster_data(c1, c2)

  if num_data:
    # initialize counts of pairwise measures
    a = 0.0  # same in c1, same in c2
    b = 0.0  # same in c1, different in c2
    c = 0.0  # different in c1, same in c2
    for i in range(num_data-1):
      for j in range(i+1,num_data):
        if c1[i]==c1[j] and c2[i]==c2[j]:
          a += 1
        elif c1[i]==c1[j] and c2[i]!=c2[j]:
          b += 1
        elif c1[i]!=c1[j] and c2[i]==c2[j]:
          c += 1
    # jaccard_index = ratio of both pairs matching to all pairs matching in either or both c1, c2
    if a + b + c == 0.0:
      return float('inf')
    else:
      return a/(a+b+c)
  else:
    return -1

"""