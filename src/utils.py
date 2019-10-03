# coding: utf-8

from functools import cmp_to_key
import locale
import string
locale.setlocale(locale.LC_ALL, 'pt_PT.UTF-8')

## Reduce algorithm: essentially, the two lists of tuples passed as arguments are merged into 
# one, summing the second element of tuples whose first element matches
# [('a',1),('c',2)],[('a',2), ('b',1)] => [('a',3), ('b', 1), ('c', 2)]

def reduce(list_one, list_two): 
    reduced_list = list_one

    for t_two in list_two:
        reduced_list.append(t_two)

    i=0
    while i < len(reduced_list):
        if i == len(reduced_list)-1:
            break
        t_one = reduced_list[i]
        j = i+1
        while j < len(reduced_list):
            t_two = reduced_list[j]
            if t_one[0] == t_two[0]:
                t_one[1] = t_one[1] + t_two[1]
                reduced_list.pop(j)
            else:
                j = j + 1
        i = i + 1

    return sort_reduced_list(reduced_list)

## Returns the first element of a an enum (list/tuple)

def getKey(item):
    c = cmp_to_key(locale.strcoll)
    if type(item[0]) is list:
        return c(item[0][0])
    else:
        return c(item[0])

## Returns a sorted version of arr using the function above defined
def sort_reduced_list(arr):
    return sorted(arr, key=getKey)

## Returns a list containing the differences between the two lists passed as arguments
def Diff(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif

## Splits texts into tokens.
# A token is a short string that can be viewed as an atomic word, that can no more be logically split.
# It is used as the first element in each tuple of the lists this program handles
def tokenizer(text):
    tokens = text.lower()
    tokens = tokens.translate(str.maketrans('', '', string.digits))
    tokens = tokens.translate(str.maketrans('', '', string.punctuation))
    tokens = tokens.translate(str.maketrans('', '', '[«»]'))

    tokens = tokens.rstrip()
    return tokens.split()
