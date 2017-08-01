### CIND719 Assignment 4
##### Written by: LEE, MUN KYU

#### Question 1
##### Count the number of even and odd numbers in the file

```python
nums = sc.textFile("/user/lab/number_list.txt").map(lambda x: int(x) )

# Filter and count the even numbers
even = nums.filter(lambda x: x % 2 == 0)
even.count()

# Filter and count the odd nubmers
odd = nums.filter(lambda x: x % 2 != 0)
odd.count()
```

##### Console Output:
```bash
>>> even.count()
521
>>> odd.count()
479
```


#### Question 2
##### Find 10 words with the highest count and 10 words with the lowest count

```python
word_count = sc.textFile("/user/lab/shakespeare.txt") \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1) ) \
    .reduceByKey(lambda a, b: a + b)

# Find top 10 words with most count
word_count.takeOrdered(10, lambda (k, v) -v)

# Find bottom 10 words with least count
word_count.takeOrdered(10, lambda (k, v) v)
```

##### Console Output:

```bash
>>> word_count.takeOrdered(10, lambda (k, v) -v)
[(u'the', 23407),
(u'I', 19540),
(u'and', 18358),
(u'to', 15682),
(u'of', 15649),
(u'a', 12586),
(u'my', 10825),
(u'in', 9633),
(u'you', 9129),
(u'is', 7874)]

>>> word_count.takeOrdered(10, lambda (k, v) v)
[(u'considered-', 1),
(u'mustachio', 1),
(u'protested,', 1),
(u'offendeth', 1),
(u'instant;', 1),
(u'Sergeant.', 1),
(u'nunnery', 1),
(u'swoopstake', 1),
(u'unnecessarily', 1),
(u'out-night', 1)]
```

#### Question 3
##### Count number of tweets for each user_id and save the results

```python
# Split the textfile by the delimiter and store only the user_id column
user_id = sc.textFile("/user/lab/full_text.txt") \
    .map(lambda line: line.split("\t")) \
    .map(lambda x: x[0])

# Count each distinct user_id and its count
count_user_id = user_id.map(lambda id: (id, 1) ) \
    .reduceByKey(lambda a, b: a + b)

# Save results as a text file
count_user_id.coalesce(1,true).saveAsTextFile("/user/lab/")
```

##### Result Preview:
```txt
(u'USER_42fe4a4a', 20)
(u'USER_e3ce1c03', 20)
(u'USER_c5e85528', 27)
(u'USER_7db16430', 28)
(u'USER_550a2a1d', 26)
(u'USER_9275ea04', 40)
(u'USER_6244af88', 49)
(u'USER_cc0a7d67', 23)
(u'USER_09dbf5de', 98)
(u'USER_73dcbc65', 29)
...
```
