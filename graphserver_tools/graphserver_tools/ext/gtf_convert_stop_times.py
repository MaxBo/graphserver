#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Max Bohnet
#
# Created:     26/06/2012
# Copyright:   (c) Max Bohnet 2012
#-------------------------------------------------------------------------------
#!/usr/bin/python
# -*- coding: utf-8 -*-
import codecs
def main():
    with codecs.open(r"C:\Users\Max_2\Desktop\mp\stop_times.txt", "w", "utf-8") as f1:
        f = open(r'C:\Users\Max_2\Desktop\mp\stop_times0.txt')
        f1.write(f.readline())
        for line in f.readlines():
            l = line.split(',')
            if l[1]:
                l[1]+=':00'
            else:
                if l[2]:
                    l[1] = l[2] + ':00'
            if l[2]:
                l[2]+=':00'
            else:
                if l[1]:
                    l[2] = l[1]
            if not l[1] and not l[2]:
                l[1] = '00:00:00'
                l[2] = '00:00:00'
                l[5] = '1'
                l[6] = '1\n'

            l2 = ','.join(l)
##            if l[1] and l[2] and not (l[5] == '1' and l[6].strip() == '1'):
            f1.write(l2)

if __name__ == '__main__':
    main()
