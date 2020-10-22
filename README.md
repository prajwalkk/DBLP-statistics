## CS441 - CS 441 - Engineering Distributed Objects for Cloud Computing

## Homework 2 - DBLP-Statistics

---

### Overview

1. The objective of this homework was to process the [DBLP](https://dblp.uni-trier.de/) dataset using **Hadoop Map-Reduce framework** to find out various statistics. Each Job is named appropriately like: Job1, Job2, Job3, Job4, Job5
2. EMR Deployment Link: https://youtu.be/ou1ybwYI7Ec

### Instructions

#### Environment

The project was developed using the following environment:

- **OS:** Windows 10
- **IDE:** IntelliJ IDEA Ultimate 2018.3
- **Hypervisor:** VMware Workstation 16 Pro
- **Hadoop Distribution:** [Hortonworks Data Platform (3.0.1) Sandbox](https://hortonworks.com/products/sandbox/) deployed on VMware

#### Prerequisites

- [HDP Sandbox](https://hortonworks.com/products/sandbox/) set up and deployed on (VMware or VirtualBox). Read this [guide](https://hortonworks.com/tutorial/learning-the-ropes-of-the-hortonworks-sandbox/) for instructions on how to set up and use HDP Sandbox
- Ability to use SSH and SCP on your system
- [SBT](https://www.scala-sbt.org/) installed on your system
- [dblp.xml](https://dblp.uni-trier.de/xml/) downloaded on your system

#### Running the map reduce job

1. Clone this repository
2. Browse to the project directory. The code files are in the DBLP-statistics folder. Import that folder in intelliJ
3. Generate the fat jar file using SBT on shell / cmd

```
sbt clean compile assembly
```

if the system has WSL, do this to copy files to WSL else do not do it.
`sbt deploy`

4. Start HDP sandbox VM
5. Copy the jar file to HDP Sandbox VM

```
scp -P 2222 ./target/scala-2.13/DBLP-statistics-assembly-0.1.jar maria_dev@sandbox-hdp.hortonworks.com:~/
```

6. Copy `dblp.xml` to HDP Sandbox

```
scp -P 2222 /path/to/dblp.xml maria_dev@sandbox-hdp.hortonworks.com:~/
```

7. SSH into HDP Sandbox

```
ssh -p 2222 maria_dev@sandbox-hdp.hortonworks.com
```

8. Create input directory on HDFS and copy `dblp.xml` there

```
hdfs dfs -mkdir -p /user/maria_dev/Input/
hdfs dfs -put dblp.xml /user/maria_dev/Input/

```

specify whatever input directory you wish

9. Start the map-reduce job. You can specify one or more `Job<n>` tags

```
hadoop jar DBLP-statistics-assembly-0.1.jar /user/maria_dev/Input/ /user/maria_dev/Output/ Job1 Job2 Job3 Job4 Job5
```

### Exploratory analysis of DBLP

#### Some grep Commands Run

These were basic shell scripts to make sense of the XML

1. Number of authors and editors. Just a ballpark. No filtering

```
prajwalkk@PRAJWALKK:~$ grep --count '<author' dblp.xml
18716385
prajwalkk@PRAJWALKK:~$ grep --count '<editor' dblp.xml
113016
```

2. Check the tags that precede `<author>` tag
   
```
prajwalkk@PRAJWALKK:~$ grep -B1 '<author>' dblp.xml | grep -vE '<author |^--$' | grep -oE '<(\w+) ?' | sort | uniq
<article
<author
<book
<cdrom
<crossref
<editor
<ee
<ee
<incollection
<inproceedings
<mastersthesis
<note
<phdthesis
<proceedings
<sub
<sup
<title
<url
<www
```
3. Editors also can be considered as author. Looking at the tags that precede editor.

```
prajwalkk@PRAJWALKK:~$ grep -B1 '<author>' dblp.xml | grep -vE '<author |^--$' | grep -oE '<(\w+) ?' | sort | uniq
<article
<author
<book
<cdrom
<crossref
<editor
<ee
<ee
<incollection
<inproceedings
<mastersthesis
<note
<phdthesis
<proceedings
<sub
<sup
<title
<url
<www
```

We only need these tags that specify publications `article|inproceedings|proceedings|book|incollection|phdthesis|mastersthesis|www|person|data`

### Some observations and assumptions

#### 1. Spreadsheet or a CSV file that shows top ten published authors at each venue.

To run this job:

```
hadoop jar DBLP-statistics-assembly-0.1.jar /user/maria_dev/Input/ /user/maria_dev/Output/ Job1
```

1.  The jobs takes in the XML individual tag as input.
2.  The Mapper phase has `key, value` pairs as `venue, author`
3.  In the reducer phase, we generate reverse ordering of value `author`
    based off its frequency of occourence in the list of values. The
    list is sliced to take only 10 values.
4.  The sample output is shown below. First field is Venue, Next are the
    authors with thier pulication count in descending order.

```
     1  #MSM,Aba-Sah Dadzie(9),Milan Stankovic(8),Matthew Rowe 0001(7),Amparo Elizabeth Cano Basave(4),Giuseppe Rizzo 0002(3),Markus Strohmaier(3),Andrea Varga(3),Zhemin Zhu(2),Amparo Elizabeth Cano(2),Mena B. Habib(2)
     2  #Microposts,Katrin Weller(2),Kara Greenfield(2),Kelly Geyer(2),Alyssa C. Mensch(2),Danica Radovanovic(2),Aba-Sah Dadzie(2),Olga Simek(2),Harald Sack(1),Nemanja Djuric(1),Marieke van Erp(1)
     3  *SEM@COLING,Sabine Schulte im Walde(2),Deepak P 0001(1),James Pustejovsky(1),Torsten Zesch(1),Nitish Aggarwal(1),Anders Johannsen(1),Lucy Vanderwende(1),Anh Tran(1),Barbara Plank(1),Steve L. Manion(1)
     4  *SEM@NAACL-HLT,Benjamin Van Durme(7),Mona T. Diab(6),Rada Mihalcea(6),Timothy Baldwin(6),Eneko Agirre(5),Dan Roth(5),Anette Frank(5),Ido Dagan(4),Steven Bethard(4),Sebastian Padó(4)
     5  10th Anniversary Colloquium of UNU/IIST,Bernhard K. Aichernig(2),T. S. E. Maibaum(2),Chris George(1),Naoki Kobayashi 0001(1),Paul A. Bailes(1),Alexandre David(1),Natarajan Shankar(1),José Luiz Fiadeiro(1),Markus Kaltenbach(1),Dang Van Hung(1)
     6  25 Years GULP,Paolo Torroni(2),Frank D. Valencia(1),Annalisa Bossi(1),Marco Gavanelli(1),Alberto Momigliano(1),María Alpuente(1),Sergio Greco(1),Gianfranco Rossi(1),Catuscia Palamidessi(1),Matteo Baldoni(1)
     7  3D Multiscale Physiological Human,Nadia Magnenat-Thalmann(3),Daniel Thalmann(2),Gavin Olender(1),Jan Rzepecki(1),Sara Trombella(1),Pascal Perrier(1),Sean Lynch(1),Ian Stavness(1),Karelia Tecante(1),Joaquim Miguel Oliveira(1)
     8  3D Research Challenges in Cultural Heritage,Sander Münster(2),Dieter W. Fellner(2),Jennifer von Schwerin(1),Mario E. Santana-Quintero(1),Daniel Thalmann(1),Patrick Callet(1),Oliver Hauck(1),Fabrizio Ivan Apollonio(1),Cindy Kröber(1),Moritz Neumüller(1)
     9  3D-GIS,Alias Abdul-Rahman(5),Ayman F. Habib(4),Volker Coors(3),Francesco Fassi(3),Sisi Zlatanova(3),Weihong Cui(2),Mohammad Reza Malek(2),Jan Kolár(2),Wenzhong Shi(2),Jiann-Yeou Rau(2)
    10  3DCVE@VR,Thierry Duval(6),Valérie Gouranton(4),Bruno Arnaldi(4),Morgan Le Chénéchal(3),Jérôme Royan(3),Florian Nouviale(1),Sascha Gebhardt(1),Nicolas Ladeveze(1),Bernd Hentschel 0001(1),Anderson Maciel(1)
```

#### 2. Compute the list of authors who published without interruption for N years where 10 <= N

To run this job:

```
hadoop jar DBLP-statistics-assembly-0.1.jar /user/maria_dev/Input/ /user/maria_dev/Output/ Job2
```

1.  The jobs takes in the XML individual tag as input.
2.  The Mapper phase has `key, value` pairs as `author, year`
3.  In the reducer phase, the list of years are sorted, made into a set of distinct elements. Then the maximum continuous range is calculated here. If the max value is more than 10, then it is emitted as reducer output
4.  The sample output is shown below. First field is author, second is number of continuous years. It is been sorted reverse by `sort -unr -k2 -t, Job2/continuous_n_years.csv | head -n20` . Note that, the original output is not sorted.

```
   1   Shi-Kuo Chang,51
   2   Arto Salomaa,50
   3   Ronald L. Rivest,49
   4   Béla Bollobás,48
   5   Stephen S. Yau,47
   6   Hanan Samet,46
   7   Christos H. Papadimitriou,45
   8   John H. Reif,44
   9   Bruno Courcelle,43
   10  Averill M. Law,42

```

#### 3. Generating publications with one author in each venue

To run this job:

```
hadoop jar DBLP-statistics-assembly-0.1.jar /user/maria_dev/Input/ /user/maria_dev/Output/ Job3
```

1. The job takes in the XML individual tag as input.
2. The Mapper phase has `key, value` pairs as `venue, publication`. Here the k,v pairs are written into file only if the author count is 1
3. In the reducer phase, all the single author publications are concatenated onto a single string and written as the output
4. The sample output is shown below. First field is author, second is number of continuous years. It is been sorted reverse by `

```
     1  #MSM,Computational Social Science and Microblogs - The Good, the Bad and the Ugly.||Information Theoretic Tools for Social Media.||ACE: A Concept Extraction Approach using Linked Open Data.||Unsupervised Information Extraction using BabelNet and DBpedia.||A New ANEW: Evaluation of a Word List for Sentiment Analysis in Microblogs.
     2  #Microposts,Studying the Role of Elites in U.S. Political Twitter Debates.
     3  *SEM@COLING,Learning the Peculiar Value of Actions.||Identifying semantic relations in a specialized corpus through distributional analysis of a cooccurrence tensor.
     4  *SEM@NAACL-HLT,Lexical semantic typologies from bilingual corpora - A framework.||Enthymemetic Conditionals.||Non-atomic Classification to Improve a Semantic Role Labeler for a Low-resource Language.||The complexity of finding the maximum spanning DAG and other restrictions for DAG parsing of natural language.||Simple and Phrasal Implicatives.||IBM_EG-CORE: Comparing multiple Lexical and NE matching features in measuring Semantic Textual similarity.||Adaptive Clustering for Coreference Resolution with Deterministic Rules and Web-Based Language Models.||Metaphor Identification as Interpretation.||UWashington: Negation Resolution using Machine Learning Methods.||FBK: Exploiting Phrasal and Contextual Clues for Negation Scope Detection.||An Evaluation of Graded Sense Disambiguation using Word Sense Induction.||"Could you make me a favour and do coffee, please?": Implications for Automatic Error Correction in English and Dutch.||More Words and Bigger Pictures.||#Emotional Tweets.||SRIUBC-Core: Multiword Soft Similarity Models for Textual Similarity.||Towards a Formal Distributional Semantics: Simulating Logical Calculi with Tensors.
     5  10th Anniversary Colloquium of UNU/IIST,Where, Exactly, Is Software Development?||X2Rel: An XML Relation Language with Formal Semantics.||A Formal Basis for Some Dependability Notions.||"What Is an Infrastructure?" Towards an Informatics Answer.||Type Systems for Concurrent Programs.||Verification by Abstraction.||Real-Time Process Algebra and Its Applications.||Coordination Technologies for Just-in-Time Integration.||In Memoriam Armando Martín Haeberer: 4 January 1947 - 11 February 2003.||An Algebraic Approach to the VERILOG Programming.||Towards the Verifying Compiler.||Contract-Based Testing.||Multi-view Modeling of Software Systems.||UNU and UNU/IIST.||The Development of the RAISE Tools.||Real-Time Systems Development with Duration Calculi: An Overview.||A Grand Challenge Proposal for Formal Methods: A Verified Stack.
```

#### 4. Publication at each venue with highest authors

To run this job:

```
hadoop jar DBLP-statistics-assembly-0.1.jar /user/maria_dev/Input/ /user/maria_dev/Output/ Job4
```

1.  The job takes in the XML individual tag as input.
2.  The Mapper phase has `key, value` pairs as `venue, (publication, num_authors) `
3.  In the reducer phase, it has a filter function. The filter filters the tuple of `publication, num_authors)` by `max(num_authors)` Whatever value(s) are the same as the max is being written in the reducer output.
4.  The sample output is shown below.

```
   1  #MSM,Making Sense of Location-based Micro-posts Using Stream Reasoning. (7)
   2  #Microposts,A Reverse Approach to Named Entity Extraction and Linking in Microposts. (9)
   3  *SEM@COLING,More or less supervised supersense tagging of Twitter. (5),Text Summarization through Entailment-based Minimum Vertex Cover. (5)
   4  *SEM@NAACL-HLT,A New Dataset and Evaluation for Belief/Factuality. (18)
   5  10th Anniversary Colloquium of UNU/IIST,A Tool Architecture for the Next Generation of Uppaal. (4)
   6  25 Years GULP,Agents, Multi-Agent Systems and Declarative Programming: What, When, Where, Why, Who, How? (5)
   7  3D Multiscale Physiological Human,Coupled Biomechanical Modeling of the Face, Jaw, Skull, Tongue, and Hyoid Bone. (7),Clinical Gait Analysis and Musculoskeletal Modeling. (7)
   8  3D Research Challenges in Cultural Heritage,Enrichment and Preservation of Architectural Knowledge. (11)
   9  3D-GIS,Development of Country Mosaic Using IRS-WiFS Data. (5),Automatic Generation of Pseudo Continuous LoDs for 3D Polyhedral Building Model. (5),Reconstruction of Complex Buildings using LIDAR and 2D Maps. (5),Macro to Micro Archaeological Documentation: Building a 3D GIS Model for Jerash City and the Artemis Temple. (5),Volumetric Spatiotemporal Data Model. (5),Research on a feature based spatio-temporal data model. (5),3D Modeling Moving Objects under Uncertainty Conditions. (5),Digital Terrain Models Derived from SRTM Data and Kriging. (5)
   10  3DCVE@VR,When the giant meets the ant an asymmetric approach for collaborative and concurrent object manipulation in a multi-scale environment. (6)

```

#### 5. Top - 100 Collaborators and 100 Individualists

To run this job:

```
hadoop jar DBLP-statistics-assembly-0.1.jar /user/maria_dev/Input/ /user/maria_dev/Output/ Job5
```

1. This is a 2 phase Mapreduce Job.
2. First MR phase calculates the scores of each author. Second phase sorts them in descending order
3. The Mapper of the first job has `key, value` pairs as `author, uniq_co_authors_list`This has 2 conditions. If the publication XML has nore than one author, all the possible permutations are generated. We know by the formula nP2 = 2 \* nC2. This generates all the bidirectional relationships between authors using the scala's `permutation` and `combination` methods. If it is a single author, a trash string is added as coauthor which is later discared in reduce phase
4. In the reducer phase, the size of co-author list for each author is calculated and witten into the file.
5. The file is then used as input for another mapreduce job which takkes in the reducer output. This job has the `key,value` as `num_co_author, author. The reducer is just a plain sorting class as it inherently sorts based on keys.
6. later these linux commands are executed to get the 2 files.

```
 grep ",0$" part-r-00000  | head -n100 > non_collaborators.csv
 head -n100 part-r-00000 > top_100_collaborators.csv
```

```
Top Collaborators:
     1  Wei Li,-3642
     2  Yang Liu,-3436
     3  Wei Wang,-3407
     4  Wei Zhang,-3337
     5  Lei Zhang,-3318
     6  Yu Zhang,-3224
     7  Lei Wang,-3039
     8  Li Zhang,-2678
     9  Xin Wang,-2623
    10  Jing Wang,-2571
```

```
Individualists:
     1  P. Gonet,0
     2  Claudius Graebner,0
     3  Ángel del Río Fernández,0
     4  Carl Engblom,0
     5  Rouhollah Tavakoli,0
     6  Yung Kyung Park,0
     7  Eran Kahana,0
     8  Iffat Jahan,0
     9  Claudius Paul,0
    10  Roulette Wm. Smith,0
```

#### Improvements for the future and Credits

1. The XMLInputFormat is been referred from Mahout's XMLInpurFormat and [Mayank Rastogi](https://github.com/mayankrastogi)'s project.
2. The Sorting job in 5th program can be imporved to be a part of single MR execution
3. The part-files need to be generated programatically rather than running `hdfs dfs -getmerge` inside of Scala program.
4. A standalone shell script that manages all the executions of copying the file to hdfs etc. to be created. 
