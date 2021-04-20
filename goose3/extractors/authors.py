# -*- coding: utf-8 -*-
"""\
This is a python port of "Goose" orignialy licensed to Gravity.com
under one or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.

Python port was written by Xavier Grangier for Recrutae

Gravity.com licenses this file
to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import re
from goose3.extractors import BaseExtractor

class AuthorsExtractor(BaseExtractor):
    AUTHOR_REPLACER = re.compile("(^by\s+)|([\|\/].+)|(\S+:)", flags=re.IGNORECASE)
    AUTHOR_SPLITTER = re.compile(r"\band\b|,", flags=re.IGNORECASE|re.U)
    BAD_AUTHOR = re.compile(r"[0-9]")

    def extract(self):
        authors = []

        if self.article.schema and \
           hasattr(self.article.schema, 'get') and \
           self.article.schema.get("author"):
            if isinstance(self.article.schema["author"], dict):
                canidate_authors = [self.article.schema["author"]]
            elif isinstance(self.article.schema["author"], list):
                canidate_authors = self.article.schema["author"]
            else:
                canidate_authors = []
            for item in canidate_authors:
                if isinstance(item, str):
                    authors.append(item)
                elif item.get("@type") == 'Person':
                    authors.append(item.get("name", ''))

        if not authors:
            author_nodes = self.parser.getElementsByTag(
                                self.article.doc,
                                attr='itemprop',
                                value='author')
            for author_node in author_nodes:
                name_nodes = self.parser.getElementsByTag(
                                author_node,
                                attr='itemprop',
                                value='name')
                if len(name_nodes) > 0:
                    name = self.parser.getText(name_nodes[0])
                    authors.append(name)
                else:
                    authors.append(self.parser.getText(author_node))

            for known_tag in self.config.known_author_patterns:
                if known_tag.xpath:
                    tags = self.parser.xpath_re(self.article.doc, known_tag.xpath)
                else:
                    tags = self.parser.getElementsByTag(
                                    self.article.doc,
                                    attr=known_tag.attr,
                                    value=known_tag.value)
                if tags:
                    if not known_tag.content:
                        author = self.parser.getText(tags[0])
                    else:
                        author = self.parser.getAttribute(
                            tags[0],
                            known_tag.content
                        )
                    authors.append(author)

            for item in self.article.microdata.get("newsarticle", []):
                author = item.get('author')
                if author:
                    authors.append(author)

            for item in self.article.microdata.get("person", []):
                author = item.get('name')
                if author:
                    authors.append(author)

            for item in self.article.microdata.get("hcard", []):
                author = item.get('n')
                if author:
                    authors.append(author)

            author = self.article.metatags.get("author")
            if author:
                authors.append(author)

        clean_authors = []
        author_keys = {}
        for full_author in authors:
            if not full_author:
                continue
            if not isinstance(full_author, str):
                continue
            for author in self.AUTHOR_SPLITTER.split(full_author):
                author = self.AUTHOR_REPLACER.sub("", author).strip()
                if not author:
                    continue
                if author.lower() in author_keys:
                    continue
                if self.BAD_AUTHOR.match(author):
                    continue
                author_keys[author.lower()] = True
                clean_authors.append(author)

        clean_authors = list(set(clean_authors))
        clean_authors.sort()
        return clean_authors
