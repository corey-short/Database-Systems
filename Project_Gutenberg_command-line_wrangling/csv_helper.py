#!/usr/bin/env python

__author__ = 'Short'

import csv
import sys


class CSVHelper():
    def __init__(self):
        self.header = ['title', 'author', 'release_date', 'ebook_id', 'language', 'body']
        self.start = '*** START OF THE PROJECT GUTENBERG EBOOK APOCOLOCYNTOSIS ***'
        self.end = '*** END OF THE PROJECT GUTENBERG'
        self.write(self.header)
        self.title_flag = False
        self.author_flag = False
        self.release_date_flag = False
        self.ebook_id_flag = False
        self.language_flag = False
        self.header_parsed = False
        self.start_flag = False
        self.end_flag = False

    def read(self):
        title = "Title:"
        author = "Author:"
        release_date = "Release Date:"
        ebook_id = ""
        language = "Language:"
        for line in sys.stdin:
            line = line.rstrip()
            if not self.start_flag and not self.header_parsed:
                if line.startswith(title):
                    title = line.split(':')[1].lstrip()
                    self.title_flag = True
                elif line.startswith(author):
                    author = line.split(':')[1].lstrip()
                    self.author_flag = True
                elif line.startswith(release_date):
                    date_and_ebook_id = line.split(':')[1].split('[')
                    release_date = date_and_ebook_id[0].strip()
                    self.release_date_flag = True
                    ebook_id = date_and_ebook_id[1].split('#')[1].strip(']')
                    self.ebook_id_flag = True
                elif line.startswith(language):
                    language = line.split(':')[1].lstrip()
                    self.language_flag = True
                elif line.startswith('*** START'):
                    self.start_flag = True
                if self.title_flag and self.author_flag and self.release_date_flag and self.ebook_id_flag and self.language_flag and self.start_flag:
                    self.header_parsed = True
                    body = "".join(line for line in sys.stdin)
                    rows = zip([title], [author], [release_date], [ebook_id], [language], [body])
                    for row in rows:
                        self.write(row)

    def write(self, row):
        filename = "ebook.csv"
        with open(filename, 'w') as out_file:
            if row:
                writer = csv.writer(out_file, quoting=csv.QUOTE_MINIMAL)
            else:
                writer = csv.writer(out_file)
            writer.writerow(self.header)
            writer.writerow(row)


def main():
    csv_helper = CSVHelper()
    csv_helper.read()


if __name__ == '__main__':
    main()
