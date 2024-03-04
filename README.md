# COMP4321 Search Engine Project

# Phase 1

- [x] Implement a spider (integrated with an indexer) for fetching (using BFS) and indexing
- [x] Index 30 pages from https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm or https://comp4321-hkust.github.io/testpages/testpage.htm (backup website)
- [x] Implement a test program which reads data from the jdbm and outputs a plain-text file named spider_result.txt. The format of the spider_result.txt file should be as follows:

```
Page title
URL
Last modification date, size of page
Keyword1 freq1; Keyword2 freq2; Keyword3 freq3; ... ...
Child Link1
Child Link2 ... ...
——————————————– (The separator line should be a line of hyphens, i.e. -)
Page title
URL
Last modification date, size of page
Keyword1 freq1; Keyword2 freq2; Keyword3 freq3; ... ...
Child Link1
Child Link2 ... ...
... ...
... ...
```

- [x] The list of keywords/child links displays up to 10, and there is no requirement for the order.

We need to submit:
- [ ] A document containing the design of the jdbm database scheme of the indexer. All supporting databases should be defined, for example, forward and inverted indexes, mapping tables for URL <=> page ID and word <=> word ID conversion. The jdbm database schema depends on the functions implemented. You should include an explanation of your design.
- [x] The source codes of the spider and the test program
- [ ] A readme.txt file containing the instructions to build the spider and the test program, and how to execute them.
- [ ] The db file(s) which contain the indexed 30 pages starting from https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm or https://comp4321-hkust.github.io/testpages/testpage.htm (backup website)
- [x] spider_result.txt, which is the output of the test program

Zip the files and submit via Canvas. The assignment name is Phase1.
