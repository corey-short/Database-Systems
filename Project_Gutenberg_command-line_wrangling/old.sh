#!/bin/bash
while read line; do
    python csv_helper.py $1
done < $1
exit 0
# python csv_helper.py $1
# echo "`python csv_helper.py CSVHelper.write`("$header")``"
# python -c 'import csv_helper; print csv_helper.CSVHelper.write("$header")'
# sed 's/^ *//'`"
# OLDIFS=$IFS
# IFS=","

#python csv_helper.py $1
#exit 0

#title="Title:"
#author="Author:"
#release_date="Release Date:"
#ebook_id=""
#language="Language:"
#body=""
#start_body=("*** START OF THE PROJECT GUTENBERG EBOOK APOCOLOCYNTOSIS ***")
#end_body=("*** END OF THE PROJECT GUTENBERG")

#if [ "$line" == "$start_body" ]; then

        #echo "------ Start of body ------"
        #header=()
        #header+="`grep "$title" $1 | cut -d\: -f2 | sed 's/^ *//'`"         # title
        #header+="`grep "$author" $1 | cut -d\: -f2`"                        # author
        # TODO: Date format incorrect.
        #header+="`grep "$release_date" $1 | cut -d\: -f2 | cut -d\[ -f1`"   # release_date
        #header+="`grep "$release_date" $1 | cut -d\# -f2 | tr -d "]"`"      # ebook_id
        #header+="`grep "$language" $1 | cut -d\: -f2`"                      # language

        #echo "$header" | python csv_helper.py
        #python csv_helper.py

    #fi
# Use < to redirect stdin to our loop. Can use to output to a file.
# 1$: Do note have to hard code the file name in

# IFS=${OLDIFS} # replace IFS with OLDIFS

#echo "`python csv_helper.py $1`"
#echo "`python test/ebook_test.py`"
