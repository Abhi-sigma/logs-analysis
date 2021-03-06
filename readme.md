
# Project Synopsis

This is the third project and is a requirement for  Udacity Full Stack Developers Nanodegree.The project
requires knowledge of bash terminal,python and sql queries to find the answers of the questions asked in project description.The
project required a single python script which could solve three questions that were asked:

+ **_`What are the most popular three articles of all time?`_**
+ **_`Who are the most popular article authors of all time?`_**
+ **_`On which days did more than 1% of requests lead to errors?`_**




## Running the Project:

Requirements:
+ **Virtual Box**
+ **Vagrant**
+ **Bash terminal(for windows machine)**


Installation:
(Recommended Method)

1. If you are using a Mac or Linux system, your regular terminal program will do just fine.On Windows, 
use the Git Bash terminal that comes with the Git software.
If you don't already have Git installed, download Git from [here](git-scm.com). 

2. VirtualBox is the software that actually runs the virtual machine. You can download it from virtualbox.org, [here](https://www.virtualbox.org/wiki/Downloads). 
Install the platform package for your operating system. You do not need the extension pack or the SDK. You do not need 
to launch VirtualBox after installing it; Vagrant will do that.

	<strong>Note to Ubuntu users:
	If you are running Ubuntu 14.04, install VirtualBox using the Ubuntu Software Center instead.
	Due to a reported bug, installing VirtualBox from the site may uninstall other software you need.</strong>

3. Vagrant is the software that configures the VM and lets you share files between your host computer 
and the VM's filesystem. Download it from (vagrantup.com). Install the version for your operating system.

Windows users: The Installer may ask you to grant network permissions to Vagrant or make a firewall exception. Be sure to allow this.


4. You can download and unzip this file: [here](https://d17h27t6h515a5.cloudfront.net/topher/2017/June/5948287e_fsnd-virtual-machine/fsnd-virtual-machine.zip)
This will give you a directory called FSND-Virtual-Machine. 
It may be located inside your Downloads folder.Alternately, you can use Github to fork and 
clone the repository from this [repository](https://github.com/udacity/fullstack-nanodegree-vm). Either way, 
you will end up with a new directory containing the VM files. Change to this directory in your terminal with cd.
Inside, you will find another directory called vagrant. Change directory to the vagrant directory:

5. Once you get your terminal(which may take quite a while) depending on your internet speed.Type `vagrant up`

6. SSH into virtual machine by typing `vagrant ssh`.

7. change directory to `cd /vagrant`

8. Download the data [here](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip),unzip the file and put the file newsdata.sql into vagrant diretory.

9. Load the data by typing `psql -d news -f newsdata.sql`.This will create 'news' database and three tables 'articles','authors' and 'log'.

10. Quit the psql prompt by typing `\q` or pressing <kbd>CTRL</kbd> + <kbd>D</kbd>

11. cd into vagrant directory and type `git clone https://github.com/Abhi-sigma/logs-analysis`.
This will clone the logs-analysis git diretory into the vagrant directory.cd into this directory and 
type`psql -d news -f create_view.sql`.You will see "CREATE VIEW" on the terminal symbolising successful creation of a view.
Now again type `python queries.py`.Please note the main script queries.py depends on these views so the python script will throw error if you dont run these first.
If everything in followed exactly from above steps,you will see output in terminal as in output.txt file in logs-analysis directory.


## About SQL VIEWS CREATED

Two views namely requests and popular_article are created, when you run psql -d news create_view.sql.You can check them out by connecting to news database at psql prompt and using select statements once you  had run the script.

Sql query which creates these view:
 
 ### popular_article View
 -------------------------------------------------------------------------------------
 CREATE OR REPLACE VIEW popular_article AS
 SELECT articles.title AS TITLE,authors.name AS NAME,
 COUNT(articles.title) AS VIEWS FROM articles,log,authors
 WHERE articles.author=authors.id AND log.path = '/article/' || articles.slug 
 GROUP BY articles.title,authors.name ORDER BY VIEWS DESC;


 ------------------------------------------------------------------------------------------------
  ### requests View

 CREATE OR REPLACE VIEW REQUESTS AS
 SELECT all_request.datet, all_request.total_request,
 request_fails.failures FROM 
 (SELECT (DATE(log.time))
 AS DATET,COUNT(DATE(log.time)) AS TOTAL_REQUEST
 FROM LOG GROUP BY DATE(log.time)) AS ALL_REQUEST
 JOIN
 (SELECT DATE(log.time) AS DATEF,COUNT(log.status)
  AS FAILURES FROM log WHERE log.status !='200 OK'
  GROUP BY DATE(log.time),log.status) AS REQUEST_FAILS
  ON all_request.datet = request_fails.datef ORDER BY all_request.datet;

 ------------------------------------------------------------------------------------







 
