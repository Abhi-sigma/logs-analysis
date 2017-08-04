# !/usr/bin/env python
# -*- coding: UTF-8 -*-


class Project_Query_Maker():

    """
    Main class that has methods which

    queries the database. This class initialises database

    and has methods to call queries.

    """

    def __init__(self):

        """Assign all the  class variables"""

        # conn_string is the parameter that holds all parameters for
        # the connection to the database

        self.conn_string = """ host = 'localhost' dbname ='news'
            user ='vagrant' password ='password' """

        # query for popular author

        self.query_popular_author = """ SELECT name,SUM(views) AS TOTAL_VIEWS
        FROM popular_article GROUP BY NAME ORDER BY TOTAL_VIEWS DESC;"""
        
        # query for popular_article
        
        self.query_popular_article = """ SELECT title,views FROM popular_article
        LIMIT 3;"""

        # sets class variable error_percentage which is overridden when
        # calling method errors as a parameter to the method.

        self.error_percentage = ""

        # connects to the database

        self.conn = self.connect_database(self.conn_string)

    def connect_database(self, conn_string):

        """Method that connects to the database

           Args:
           self
           conn_string:(string)parameters to connect to the database
        """
        import psycopg2
        conn = psycopg2.connect(conn_string)
        return conn

    def popular_article(self):

        """method that queries and returns top three articles
           prints out the top three articles on the console with
           number of views

           Args:self
        """

        cursor = self.conn.cursor()
        result = cursor.execute(self.query_popular_article)
        result = cursor.fetchall()
        count = 1
        print "Most Popular Article"
        print ".....................\n"
        for items in result:
            print ('{} . {} ------> Views {}'.format(
                count, items[0], items[1]))
            count += 1
        print "-" * 50
        return result

    def popular_author(self):

        """Method to query most popular author
        returns result in a list
        also prints out most popular author
        with most
        views at top and so on.

        Args:self
        """
        print "Most Popular Author"
        print ".......................\n"
        cursor = self.conn.cursor()
        result = cursor.execute(self.query_popular_author)
        result = cursor.fetchall()
        count = 1
        for items in result:
            print ('{}. {}---------->Views {}'.format(count,
                   items[0], items[1]))
            count += 1
        print "-" * 50
        return result

    def errors(self, error_percentage):

        """ Method to query  for  days which resulted in
            errors more than error_perentage

            Args:(float)error_perentage e.g if you are want to query for days
            which resulted in mote than 20% errors, then pass 0.2(20/100) as a
            a argument to this function.
              """
        print "Days which resulted in" + \
        "  " + '{0:.2%}'.format(error_percentage) + "  " + "errors"
        print ".............................\n"
        error_percentage = str(error_percentage)
        cursor = self.conn.cursor()
        query = """ SELECT datet,failures,total_request,failures::numeric/total_request
                    AS ERRORS_PERCENT FROM REQUESTS WHERE
                    FAILURES::NUMERIC/TOTAL_REQUEST >%s;"""
        result = cursor.execute(query, [error_percentage])
        result = cursor.fetchall()
        for items in result:
            print (' Date {} Errors----->{}'.format(
                   items[0].strftime("%B, %d, %Y"),
                   '{0:.2%}'.format(items[3])))
        print "-" * 50
        return result

if __name__ == '__main__':

    # creating a instance of the class:Project_Query_Maker
    queries = Project_Query_Maker()

    # calling method popular article on the instance
    queries.popular_article()

    # calling method popular_author on the instance
    queries.popular_author()

    # calling method errors n the instance
    # and passing 0.01 i.e 1% as a parameter
    queries.errors(0.01)
