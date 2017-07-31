# !/usr/bin/env python
# -*- coding: UTF-8 -*-


class Project_Query_Maker():

    """
    This class is the main class that creates views and has methods that

    queries the database. This class initialises database,creates views

    and has methods to call queries

    """

    def __init__(self):

        """Instantiate all the  class variables"""

        # conn_string is the parameter that holds all parameters for
        # the connection to the database

        self.conn_string = """ host = 'localhost' dbname ='news'
            user ='vagrant' password ='password' """
        self.query_popular_article = """ select articles.title,
        count(articles.title) from articles,log,authors where
        articles.author=authors.id and substring
        (articles.title,1,3) ilike substring(log.path,10,3)
        group by articles.title order
        by count desc; """

        # query to create views popular_article

        self.create_view_popular_article = """create or replace view popular_article as
        select articles.title as title,authors.name as name,
        count(articles.title) as views from articles,log,authors
        where
        articles.author=authors.id and articles.slug
        ilike substring(log.path,10)
        group by articles.title,authors.name order by views desc;"""

        # query for popular author

        self.query_popular_author = """ select name,sum(views) as total_views
        from popular_article group by name order by total_views desc;"""

        # query to create view requests

        self.create_view_requests = """ create or replace view requests as
        select all_request.datet,all_request.total_request,
        request_fails.failures from (select (date(log.time))
        as datet,count(date(log.time)) as total_request
        from log group by date(log.time)) as all_request
        join
        (select date(log.time) as datef,count(log.status)
        as failures from log where log.status !='200 OK'
        group by date(log.time),log.status)
        as request_fails
        on all_request.datet = request_fails.datef
        order by all_request.datet;"""

        # sets class variable error_percentage which is overridden when
        # calling method errors as a parameter to the method.

        self.error_percentage = ""

        # query for error where failures more than error percentage
        # error percentage is passed in method error as decimal,e.g 1% as 0.01.

        self.query_error_date = """select datet,total_request from
        requests where failures::numeric/total_request>"""

        self.conn = self.connect_database(self.conn_string)

        # calls the create views method
        # inside the init
        # so that view is created before queried

        self.create_views(self.conn, self.create_view_popular_article,
                          self.create_view_requests)

        # method to call popular article
        # returns a list of tuples
        # in descending order of views
        # also prints out top three articles

    def connect_database(self, conn_string):
        import psycopg2
        conn = psycopg2.connect(conn_string)
        print "connection established"
        return conn


# method which creates views
# this is called when instance is initialized

    def create_views(self, conn, query1, query2):
        cursor = self.conn.cursor()
        print "creating views.."
        cursor.execute(query1)
        cursor.execute(query2)
        conn.commit()
        print "finished creating views..\n"
        return

    def popular_article(self):
        cursor = self.conn.cursor()
        result = cursor.execute(self.query_popular_article)
        result = cursor.fetchall()
        count = 1
        print "Most Popular Article"
        print ".....................\n"
        for items in result:
            print str(count) + "." + str(items[0]) +\
              " ----------------> " + "Views" + " " + str(items[1])
            count += 1
            if count > 3:
                print "\n"
                return result

        # method to query most popular author
        # returns result in a list
        # also prints out most popluar author
        # with most
        # views at top and so on.

    def popular_author(self):
        print "Most Popular Author"
        print ".......................\n"
        cursor = self.conn.cursor()
        result = cursor.execute(self.query_popular_author)
        result = cursor.fetchall()
        count = 0
        for items in result:
            print str(count + 1) + " " + items[0] + "--------------> " + \
                  "Views" + "  " + str(items[1]) + "\n"
            count += 1
        return result

        # method to query requests which resulted in errors
        # the error percentage is passed as decimal to the method

    def errors(self, error_percentage):
        print "Errors"
        print ".............................\n"
        error_percentage = str(error_percentage)
        cursor = self.conn.cursor()
        query = """select datet,failures,total_request from requests where
        failures::numeric/total_request>%s;"""
        result = cursor.execute(query, [error_percentage])
        result = cursor.fetchall()
        errors = float(result[0][1])
        requests = result[0][2]
        print "Date" + " " + str(result[0][0]) + \
              "------------>" + "Errors" + " " + \
              str(round(errors / requests * 100, 2)) + "%" + "\n"
        return result

# creating a instance of the class:Project_query_maker
queries = Project_Query_Maker()

# calling method popular article on the instance
queries.popular_article()

# calling method popular_author on the instance
queries.popular_author()

# calling method errors n the instance
# and passing 0.01 i.e 1% as a parameter
queries.errors(0.01)
