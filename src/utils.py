# coding: utf-8

from functools import cmp_to_key
import locale
import string
locale.setlocale(locale.LC_ALL, 'pt_PT.UTF-8')

def reduce(list_one, list_two): # O(n²) - TODO: Change to O(n*log(n)) - works tho
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

def getKey(item):
    c = cmp_to_key(locale.strcoll)
    if type(item[0]) is list:
        return c(item[0][0])
    else:
        return c(item[0])

def sort_reduced_list(arr):
    return sorted(arr, key=getKey)

def Diff(li1, li2):
    # if li1 != None and li2 != None and li1 != [] and li2 != []:
    #     return (list(set(li1) - set(li2)))
    # else:
    #     if li1 == None and li2 == None:
    #         return []
    #     elif li1 == None:
    #         return li2
    #     elif li2 == None:
    #         return li1

    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif

def tokenizer(text):
    tokens = text.lower()
    tokens = tokens.translate(str.maketrans('', '', string.digits))
    tokens = tokens.translate(str.maketrans('', '', string.punctuation))
    tokens = tokens.translate(str.maketrans('', '', '[«»]'))

    tokens = tokens.rstrip()
    return tokens.split()
