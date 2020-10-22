## Git Commands Run

Video Link: https://youtu.be/ou1ybwYI7Ec
These were basic shell scripts to make sense of the XML

1. Number of authors and editors. Just a ballpark. No filtering

   ```console
   prajwalkk@PRAJWALKK:~$ grep --count '<author' dblp.xml
   18716385
   prajwalkk@PRAJWALKK:~$ grep --count '<editor' dblp.xml
   113016
   ```

2. Check the tags that precede `<author>` tag
   ```console
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
3. Editors also can be considered as author. Looking at the tags that precede editor

   ```console
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

Key specified in the above publication tags are of the unix style tag1/tag2/Tag3\* format. Tag2 will be considered as venue. If not present, none will be written
