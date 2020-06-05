# Data Aggregator
A repository to collect/aggregate different types of data, mainly unstructured (e.g. text), from a variety of sources.

## List of data
### Wikipedia
1. Download the most recent Wikipedia data dump [here](https://meta.wikimedia.org/wiki/Data_dump_torrents#English_Wikipedia) or [here](https://dumps.wikimedia.org/enwiki/latest/)
2. Use [Wikipedia_Extractor](http://wiki.apertium.org/wiki/Wikipedia_Extractor) python script to extract text of wikipedia articles from the data dump. Put the `WikiExtractor.py` next to the wikipedia dump (**Note** there is no need to extract the wikipedia dump zip file, the script supports `bz2` format). 
3. After running is done, all articles will be stored in a single file named `wiki.txt`. Format of the returned text file is like the following:

```
[new line]
Page 1 title:tags (tags, if available)
Page 1 url
Page 1 id
Article 1 context
[new line]
Page 2 title:tags (tags, if available)
Page 2 url
Page 2 id
Article 2 context
[new line]
...
```
4. Now, by calling the `create_wiki_db_v3()` method in [wiki_aggregator.py](data-aggregator/wiki_aggregator.py), a table of all wikipedia pages in `wiki.txt`, named `pages`, will be created in a database named `wiki`.
