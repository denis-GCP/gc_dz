"""
 ------------- gc_dz.py version 1.00.04 October 2018 ------------
 This module handles all the server-side active content for the gc-dz.com 
 website.  It is called using the WSGI interface via the gc-dz.com/exec alias.  
 The entry point for /exec calls is the application() function.  This returns
 HTML or plain text output to the website.  The text output may include JSON
 stringified objects.  The module handles GET and POST requests, as well as
 calls via Ajax made from jquery. Main functions within the module are selected
 via the m= parameter.  This is intepreted via the page_selector() function.  
 Ajax calls are referred instead to the ajaxHandler() function to identify the 
 specific task required.
"""
# Handles all website server requests via the /exec alias 
 
# CGI interface functions for Python
# plus other common utilities
import cgi, traceback, json
import os, sys, re, datetime
import random, string

# Python modules for the email system and encryption
from smtplib import SMTP
from email.utils import formatdate
from cryptography.fernet import Fernet
import hashlib

# postgreSQL interface module
import psycopg2
import psycopg2.extras

# local modules
import dbauth

def application(environ, start_response):
    """
    entry point called by Apache 2.4 via mod_wsgi, aliased as gc-dz.com/exec
    note the returned string must be of type byte, in [] wrapper otherwise 
    you get a server error, with diagnostics only to Apache error log.
    """
    try:
        status = '200 OK'
        html = ""
        contentType = "text/html"
        # list of relevant environment variables
        relenvars = ['REMOTE_ADDR', 'REQUEST_SCHEME', 'SERVER_NAME', 'REQUEST_URI',
                     'REQUEST_METHOD', 'QUERY_STRING', 'SCRIPT_FILENAME', 'SCRIPT_NAME']
        # permitted remote addresses with HTTP (any allowed for HTTPS)
        # GC office, My house 
        permaddr = ['81.130.191.66','86.24.193.27']  
        # set link back to home page - available globally
        global homeURL
        homeURL = '%s://%s' % (environ['REQUEST_SCHEME'], environ['SERVER_NAME'])    
        # check if this is an Ajax request.  If so, go to the Ajax handler
        if environ.get('HTTP_X_REQUESTED_WITH','') == 'XMLHttpRequest':
            html = ajaxHandler(environ)
            #html = "<P>Here I am by Ajax!</P>"
        # otherwise continue processing as a GET/POST request    
        elif (environ['REMOTE_ADDR'] not in permaddr) and (environ['REQUEST_SCHEME'] != "https"):
            # originating IP address not on permitted list - show error page
            #raise RuntimeError("Blocked user!!" )
            html = error_page("Access not allowed (%s://%s )" % (environ['REQUEST_SCHEME'], environ['REMOTE_ADDR']))
        else:
            # call main page    
            html = page_selector(environ)
    except Exception as e:
        # handle any errors in code
        html += "<PRE>"    
        html += "\n\nAN ERROR OCCURRED IN THE PROGRAM\n\n"
        html += traceback.format_exc()
        html += "\n\nENVIRONMENT VARIABLES:\n"
        # Sorting and stringifying the environment key, value pairs
        for key in relenvars: 
            html += '%s: %s\n' % (key, environ[key])
        # dump any post fields    
        contentLength = int(environ.get('CONTENT_LENGTH', 0))
        if contentLength>0:
            # retrieve the POST data (requestBody) from WSGI interface
            requestBody = environ['wsgi.input'].read(contentLength)
            requestBody = requestBody.decode('utf-8')
            html += "\n\nPOST string:\n"
            html += requestBody
            # convert it into a list of lists
            postFields = cgi.parse_qs(requestBody)
            html += "\n\nPOST fields:\n"
            # trap sensibly any errors loopong through list of lists
            try:
                # list of POST fields
                for field in postFields.keys():
                    # each field is represented as a list of 1 or more items
                    for f in postFields[field]:
                        html += '%s = %s\n' % (field, f) 
            except Exception as e:
                # if error occurs in above, just show traceback
                html += traceback.format_exc()
        html += "\n</PRE>"    
        #contentType = "text/plain"
    # return results to Apache server via mod_wsgi interface
    # Note 'output' must be a string of bytes, not unicode.  Other strings should be 
    # unicode (Python3 default)          
    output = str.encode(html)
    response_headers = [('Content-type', contentType),
                        ('Content-Length', str(len(output)))]
    start_response(status, response_headers)
    return [output]
    
def page_selector(environ):
# checks login status and selects pages to display based, based on u and m parameters
    params = cgi.parse_qs(environ['QUERY_STRING']) 
    # check u parameter for session ID
    sid = params.get('u', ['0'])[0]
    # valid session IDs must be 16 characters a-zA-Z0-9
    test_ptn =  re.compile('[a-zA-Z0-9\_]{16,16}')
    if test_ptn.match(sid) is None:
        # badly formed session ID
        html = error_page("Incorrectly formed Session ID - Return to home page and log in")
        return html
    elif sid=='auto_login__sctn':
        # log temporary user for stats then open SCTN page
        ip = environ['REMOTE_ADDR'] 
        sid = logTempUser(sid, ip)
        html = SCTN_tool(sid)
        return html
    else:
        # session ID format is OK - now look up to see if a valid login
        qry = getCursor()
        qry.execute("""
            SELECT U.userid, U.permit FROM gcdz.users AS U INNER JOIN gcdz.logins AS L
            ON U.userid=L.userid WHERE sessionid=%(SID)s AND timeout IS NULL
            """, {'SID': sid})
        # if no match, session does not exist or has timed out
        if qry.rowcount<=0:
            html = error_page("Session ID expired/invalid - Return to home page and log in")
            return html
    # at this point session has been validated.  Retrieve permit flags for this user        
    flds = qry.fetchone()
    pflag = flds['permit']
    # use m parameter to select page/function
    m = params.get('m', ['0'])[0]
    if m=='test':
        # invoke trasepad query tool
        html = queryTool(environ)
    elif m=='mail':
        # test sending of smtp email
        user_setup()
        html = normal_reply("SMTP mailer test", "check email for test message")
    elif m=='sctn':
        # SCTN survey system: latform list, platform detail, survey form
        html = SCTN_sys(environ, sid)
    elif m=='sctndd':
        # SCTN data tool 
        html = SCTN_tool(sid, environ)
    else:
        # unrecognised module
        html = error_page("Requested module '%s' is not available" % m)
    # return HTML for generated page to be displayed by server
    return html


def ajaxHandler(environ):
# process ajax requests
    try:
        # get post data 
        contentLength = int(environ.get('CONTENT_LENGTH', 0))
        requestBody = environ['wsgi.input'].read(contentLength)
        requestBody = requestBody.decode('utf-8')
        postFields = cgi.parse_qs(requestBody)
        # get action ID and call approriate action based on that
        actid = postFields.get('action',['0'])[0]
        if actid=="login":
            # process login request
            usernm = postFields.get('usernm',['0'])[0]
            pwd = postFields.get('pwd',['0'])[0]
            ip = environ['REMOTE_ADDR']
            (sid, msg) = user_login(usernm, pwd, ip)
            json_str = json.dumps({'sid' : sid, 'msg' : msg, 'user': usernm})   
        elif actid=="session_check":
            # see if session ID exists and is still open
            sid = postFields.get('usernm',['0'])[0]  
            usernm = sessionCheck(sid)                
            json_str = json.dumps({'user': usernm})                 
        elif actid=="register":
            # process user registration
            json_str = json.dumps({'ok': 0, 
                'sid' : "",
                'msg' : "registration not yet implemented"})                 
        elif actid=="query":
             # process a query
            json_str = processQuery(postFields) 
        else:
            # unknown action requested   
            raise RuntimeError("Unknown Action '%s' requested" % actid)
    except Exception as e:
        # give traceback and diagnostics in plain text
        html = "<PRE>\n\nAN ERROR OCCURRED IN THE PROGRAM\n\n"
        html += traceback.format_exc()
        html += "\n\nREQUEST BODY:%s\n" % requestBody
        html += "\n</PRE>"    
        json_str = json.dumps({'html': html}) 
    # return results to Apache server via mod_wsgi interface (Application module)
    return json_str 
    
def user_login(usernm, pwd, ip):
# logs a user in.  If already logged in, closes last session and
# issues a new session ID
    # check username and password, get userid
    qry = getCursor()
    qry.execute("SELECT userid, hash FROM gcdz.users WHERE email=%(USERNM)s OR username=%(USERNM)s",
        {'USERNM': usernm})
    # if no match, user does not exist
    if qry.rowcount<=0:
        msg = "User not found!"
        sid=""
        return (sid, msg)
    # check password
    flds = qry.fetchone()
    hash = hashlib.sha256(str.encode(pwd)).hexdigest()
    hash2 = flds['hash']
    # if hashes don't match, password is wrong
    if hash != hash2 :
        msg = "Password is wrong!"
        sid=""
        return (sid, msg)
    # username and password have validated OK.    
    # close any open sessions for this user
    userid = flds['userid']
    qry.execute("UPDATE gcdz.logins SET timeout=NOW() WHERE userid=%(USERID)s AND timeout IS NULL",
        {'USERID': userid}) 
    # get new session ID
    sid = session_id()
    # create login record
    qry.execute("INSERT INTO gcdz.logins (userid, sessionid, timeon, ipaddr) VALUES (%(USERID)s, %(SID)s, NOW(), %(IP)s)",
        {'USERID': userid, 'SID': sid, 'IP': ip}) 
    if qry.rowcount!=1:
        raise RuntimeError("Insert failed : %s" % qry.query)  
    qry.connection.commit()      
    # return session ID, empty message
    msg=""
    return (sid, msg)    

def processQuery(postFields):
# processes a query and returns result as an HTML table or error message string
    try:
        # initialise record counter 
        record_counter = {'from': 0, 'to': 0, 'by': 10, 'total': 0}
        # connect to database
        qry = getCursor()
        qtext = postFields.get('query',[''])[0]
        # check if this is a SELECT query
        if re.match(r'^\s*SELECT\s',qtext, re.I):
            # yes - get the limit values
            rec_from = int(postFields.get('rec_from',['1'])[0])
            rec_by = int(postFields.get('rec_by',['10'])[0])
            # records counted from base 0, so subtract 1
            if rec_from > 0:
                rec_from -= 1
            # construct limit expression to append to SELECT    
            limit_expr = " LIMIT " + str(rec_by) + " OFFSET " + str(rec_from)
            qtext += limit_expr
            record_counter['from'] = rec_from + 1
            record_counter['to'] = rec_from + rec_by
            record_counter['by']= rec_by
        qry.execute(qtext)
        record_counter['total']= qry.rowcount
        if qry.rowcount>0:
            html = makeHTMLtable(qry)
        else:    
            html = "<P>No data returned by query.  Server message: %s</P>" % qry.statusmessage
    except (psycopg2.Error) as emsg:
        # if not able to connect or other DB error, give warning message
        html = ("<P style='font-weight: bold; color: red;'>Data query error:</P><BR><PRE>%s</PRE>" % emsg)
    json_str = json.dumps({'html': html, 
        'rec_from' : record_counter['from'],
        'rec_to' : record_counter['to'],
        'rec_by' : record_counter['by'],
        'rec_total' : record_counter['total']})          
    return json_str


def HTML_header(title = "Data Manager Development Site", extras="", params =""):
# standard HTML header for each page.  Title is inserted both as a meta tag and
# as a title at the top of the visible page, beside the GC logo. Extras should
# be complete (closed) HTML tags to be inserted in the <HEAD>...</HEAD> section
# params are GET, POST and action attributes for the <BODY> tag
    template = """
    <!DOCTYPE html>
    <HTML>
    <HEAD>
    <TITLE>Global Canopy Programme - %(title)s</TITLE>
    <LINK href="https://www.gc-dz.com/main.css" rel="stylesheet"  type="text/css">
    <SCRIPT src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></SCRIPT>
    <META charset="UTF-8">
    %(extras)s
    </HEAD>
    <BODY %(params)s>
    <a name="Top" id="Top"></a>
    <p ><img src="https://www.gc-dz.com/img/gc.jpg" border=0 height=48px width=182px>
    &nbsp;
    <span style="font-size: x-large; font-weight: bold; color: black; text-align: left; vertical-align: 20px;">
    %(title)s</span></p>
    <p >&nbsp;</p>
    <hr size=10 width=600 color="#BABF57" align="left" >
    <p >&nbsp;</p>
    """
    # substitute parameters in template
    html = template % {'title': title, 'extras': extras,  'params': params}
    return html

def HTML_footer(debug=""):
# standard HTML footer with Home link
    template = """
    <p>&nbsp;</p>
    <p><a href="%(HOME)s" title="Go to Home page">Home</a></p>
    <P id='debug'>%(DEBUG)s</P>
    </BODY>
    </HTML>    
    """
    # add debugging material if debug string is non-blank
    if debug>"":
        debug = "<HR><BR>" + debug
    # substitute link to home page and return code snippet for footer
    global homeURL
    html = template % {'HOME': homeURL, 'DEBUG': debug}
    return html
   
def queryTool(environ):
# displays page and results for Trasepad Query Tool
    # template for form with text area.
    #url = '%s://%s%s' % (environ['REQUEST_SCHEME'], environ['SERVER_NAME'], environ['REQUEST_URI'])
    template = """
    <STYLE>
    td {
        color: gray;
    }
    </STYLE>
    <TABLE>
    <TR>
        <TD style="font-weight: bold; color: black;">&nbsp;<BR>SQL query:</TD>
        <TD style="text-align: center;">
            Query history<BR>
            <BUTTON id="last_btn" title="last query used">&laquo;</BUTTON>
            <BUTTON id="next_btn" title="next query used">&raquo;</BUTTON>
        </TD>
         <TD style="text-align: right" title ="Select to recall full query text">Saved queries<BR>
            <SELECT id="qlist">
                <OPTION value="-1">-- New query --</OPTION>
                <OPTION value="0"></OPTION>
            </SELECT>
        </TD>
    </TR><TR> 
        <TD colspan="3"><TEXTAREA name="query" id="query" rows="5" cols="80" title="Enter a valid SQL query">%s</TEXTAREA></TD>
    </TR><TR>
        <TD style="text-align: right;" colspan="3">Records:
            &nbsp;from&nbsp;<INPUT type="text" size="3" id="rec_from" value="1"> 
            &nbsp;to&nbsp;<INPUT type="text" size="3" id="rec_to"> 
            &nbsp;by&nbsp;<INPUT type="text" size="1" id="rec_by" value="10"> 
            &nbsp;total&nbsp;<INPUT type="text" size="4" id="rec_total"> 
            &nbsp;
            <BUTTON id="last_rs" title="Previous set of records">&laquo;</BUTTON>
            <BUTTON id="next_rs" title="Next set of records">&raquo;</BUTTON>
        </TD>
    </TR><TR> 
        <TD style="width: 200px"><BUTTON id="OK_btn" title="Executes this query">OK</BUTTON></TD>
        <TD>&nbsp;</TD>
        <TD style="text-align: right" >
            Query <BUTTON id="save_btn" title="saves this query">save</BUTTON>
            <BUTTON id="del_btn" title="deletes this query if it is saved">delete</BUTTON>
        </TD>
    </TR>    
    </TABLE>  
    <DIV id="ajax_reply"></DIV>
    """
    # set page header
    jslib = "<SCRIPT src='js/queryTool.js'></SCRIPT>"
    html = HTML_header("Trasepad Query Tool", extras=jslib)
    # page body text - populate text area with POST'ed content
    default_text = "SELECT table_name AS tbl, column_name AS col, data_type AS typ FROM information_schema.columns WHERE table_schema ='f500'"
    html += template % default_text
    # create page footer
    html += HTML_footer()
    return html
    
def makeHTMLtable(qry, n=0, m=10, lines='LightBlue', altrows='LightCyan'):
    """
    creates an HTML table from a recordset result (psycopg2 cursor object)
    records from n to m will be displayed.  If m<n, then n to n+m-1 records will 
    be shown. If n<=0 or m<=0, all records will be shown.  Default is to show
    first 10 records (n=1, m=10).  The table is put into its own <DIV>, with 
    a local stylesheet (styles) inserted at the top if given.
    """
    if qry.rowcount==0:
        return ""       # does nothing if no rows in cursor
    html = '<hr size=2 width=600 color="#BABF57" align="left" ><BR>'
    # add local styles if given.  This must be syntactically correct css stylesheet
    # get column headings from description
    colhdr = list()
    for k in range(0, len(qry.description)):  # loop though column descriptions
        colhdr.append(qry.description[k][0])    # save column names (item 0)
    # style sheet for tables
    # for named colour codes see https://www.w3schools.com/cssref/css_colors.asp
    css_table ="""
    <style>
    table.t2 {
        font-family: arial, sans-serif;
        border-collapse: collapse;
        width: 25%;
    }
    td.t2, th.t2 {
        border: 1px solid LightBlue ;
        text-align: left;
        padding: 8px;
    }
    tr.t2:nth-child(even) {
        background-color: LightCyan ;
    }
    </style>
    """
    html += css_table
    #html += css_table % {'LINES': lines, 'ROWS': altrows}
    # create the top row of table with column headings      
    html += "<TABLE class='t2'><TR class='t2'>"  
    for col in colhdr:
        html += "<TH class='t2'>%s</TH>" % col
    html += "</TR>"    
    # check rows to be output
    pass   #not coded yet, defaults assumed
    # position cursor at first row (check does not exceed record count)
    if n>=qry.rowcount: 
        n=qry.rowcount-1
    qry.scroll(n, mode='absolute')      # start from row n (default 1)
    # fetch next m records
    rows = qry.fetchmany(m)     
    # create rest of table
    for row in rows:               
        html += "<TR class='t2'>" 
        for col in row:
            html += "<TD class='t2'>%s</TD>" % col
        html += "</TR>"    
    # return HTML for completed table
    html += "</TABLE>"
    return html           

def logTempUser(sid, ip):
    """
    Logs temporary (not logged in) user connecting via a link to public tools
    such as the SCTN Data Directory
    - User ID is zero, session ID (sid) is generic for a tool
    - Any temporary sessions more than 12 hours old are closed
    - Source IP address is logged, with time on, for stats
    - No return value
    """
    # close any sessions more than 12 hours old for user zero (not logged in)
    qry = getCursor()
    qry.execute("""UPDATE gcdz.logins SET timeout=NOW() WHERE userid=0 AND timeout IS NULL 
        AND (timeon - NOW()) >= '12 hours'""")
    # register current session
    sid = session_id()
    qry.execute("INSERT INTO gcdz.logins (userid, sessionid, timeon, ipaddr) VALUES (0, %(SID)s, NOW(), %(IP)s)",
        {'SID': sid, 'IP': ip}) 
    if qry.rowcount!=1:
        raise RuntimeError("Insert failed : %s" % qry.query)  
    qry.connection.commit()
    return sid      

def SCTN_sys(environ, sid): 
    # implements the SCTN meta-database system
    # set page header with javascript libraries, debug messages (empty)
    jslib = """
    <SCRIPT src='js/sctn.js'></SCRIPT>
    <SCRIPT src='js/cookies.js'></SCRIPT>
    """
    html = HTML_header("SCTN Meta-database Tool", extras=jslib)
    debug = ""
    # get post data 
    contentLength = int(environ.get('CONTENT_LENGTH', 0))
    requestBody = environ['wsgi.input'].read(contentLength)
    requestBody = requestBody.decode('utf-8')
    postFields = cgi.parse_qs(requestBody)
    # check if platform id set                
    platid = int(postFields.get('platid',['0'])[0])
    if platid>0:
        # show form for an individual platform
        html += SCTN_form(platid, sid) 
    else:
        # show the platform list
        html += SCTN_list(sid)
    # create page footer
    html += HTML_footer(debug)
    return html

def SCTN_list(sid):
    # generates code for a listing of the SCTN companies
    # heading row
    hdr = """
    <FORM action="/exec?m=sctn&u=%s" method="POST" id="listform">
    <INPUT type="hidden" name="platid" id ="platid" value="0">
    <STYLE>TR:nth-child(even) {background-color: #f1f1f1}</STYLE>
    <TABLE style="max-width: 1000px; text-align:left">
    <TR style="background-color: #c1c1c1">
        <TH style="width: 40px; text-align: left">ID</TH>
        <TH style="text-align: left">Platform name</TH>
        <TH style="width: 400px; text-align: left">Website</TH>
        <TH style="width: 40px; text-align: center">Data</TH>
    </TR>
    <TBODY>
    """
    html = hdr % sid
    # open database connection
    qry = getCursor()
    # get the list of platforms
    rowdata =""
    qry.execute("""SELECT p.platid, p.platname, p.platurl, s.sheetid FROM sctn.platforms AS p
    LEFT JOIN sctn.sheets AS s ON p.platid=s.platid ORDER BY platname""")
    rows = qry.fetchall()     
    # create HTML for each row
    for row in rows:               
        html += "<TR>" 
        html += "<TD>%s</TD>" % row['platid']
        html += "<TD>%s</TD>" % row['platname']
        html += "<TD><A href=\"%s\" target=_blank>%s</TD>" % (row['platurl'], row['platurl'])
        if row['sheetid'] is None:
            # no data - don't show button
            html += "<TD>&nbsp;</TD>"    
        else:        
            # has data sheet, show button with link action
            html += "<TD><INPUT type='radio' onclick='set_platform(%s)'></TD>" % row['platid']     
        html += "</TR>"
    # finish off HTML form
    html += "</TBODY></TABLE></FORM>"     
    return html

def SCTN_form(platid, sid):
    # displays a form with details for a specific platform
    # connect to database
    qry = getCursor()
    # get the list of platforms
    qry.execute("select platid, platname, platurl from sctn.platforms where platid=%s", (platid,))
    flds = qry.fetchone()
    pname = flds['platname']
    purl = flds['platurl']
    
    html = """
    <TABLE style="width: 1000px;">
        <TR><TD>Survey details for <B>%(NAME)s</B></TD>
            <TD><A href="%(URL)s" target=_blank>%(URL)s</A></TD>
            <TD align="right"><A href="/exec?m=sctn&u=%(SID)s">Back to list</A></TD></TR>
    </TABLE>        
    <BR>&nbsp;
    <STYLE>TR:nth-child(even) {background-color: #f1f1f1}</STYLE>
    <TABLE style="max-width: 1000px; text-align:left">
    <THEAD>
    <TR style="background-color: #c1c1c1">
        <TH style="width: 50px; text-align: left">No.</TH>
        <TH style="width: 300px; text-align: left">Survey question</TH>
        <TH style="width: 300px; text-align: left">Response</TH>
        <TH style="width: 300px; text-align: left">Notes</TH>
    </TR>
    </THEAD>
    <TBODY>
    """ % {'NAME': pname, 'URL': purl, 'SID': sid}
    # get sheetid for this platform (first sheet only, if duplicates)
    qry.execute("select sheetid from sctn.sheets where platid=%s order by sheetid limit 1", (platid,))
    shtid = qry.fetchone()['sheetid']
    # generate data table, first get questions and results as an array
    qry.execute("select qid, question, drow, nrow from sctn.questions order by qid")
    qlist = qry.fetchall()
    # loop through the questions
    for q in qlist:
        # add HTML for question ID and text in first two columns - span expected rows of answers
        hrow = """
        <TR>
            <TD rowspan="#?">%(QID)s</TD>
            <TD rowspan="#?">%(QUESTION)s</TD> 
        """ % {'QID': q['qid'], 'QUESTION': q['question']}
        # test for possible types of answer 
        if q['drow'] is None:
            # if answers not defined (drow is null), finish row with empty cells
            hrow += "<TD>&nbsp;</TD><TD>&nbsp;</TD></TR>"
        elif q['qid']<=14:
            # part 1 of form with categorical ansers and notes 
            # get replies from Excel columns C-D
            for xlcol in ['C', 'D']:
                qry.execute("SELECT xlcell FROM sctn.xldata WHERE xlrow=%s AND xlcol=%s AND sheetid=%s", (q['drow'], xlcol, shtid))
                if qry.rowcount>= 1:    # should be 1 or 0
                    ans = qry.fetchone()['xlcell']
                    hrow += "<TD>%s</TD>" % ans
                else:
                    hrow += "<TD>&nbsp;</TD>"
            # finish table row in HTML
            hrow += "</TR>"        
        else:
            # part 2 of form, only descriptive answers in column D, but may be several rows. 
            first = q['drow']
            last = q['drow'] + q['nrow'] - 1
            qry.execute("""SELECT xlcell AS ans FROM sctn.xldata WHERE xlrow 
            BETWEEN %s AND %s AND xlcol='D' AND sheetid=%s""", (first, last, shtid ))
            # output the rows
            n = 0
            if qry.rowcount>= 1:    # should be 1 or 0
                answers = qry.fetchall()
                for a in answers:
                    n += 1
                    if n>1:
                        # this is a 'spanned' row - add new row code in HTML #
                        hrow += "<TR>"
                    # add text for this row    
                    hrow += "<TD>&nbsp;</TD><TD>%s</TD></TR>" % a['ans']
                # add filler part-rows where number of rows fetched is less than nominal number
                if qry.rowcount < q['nrow']:
                    rows_left = q['nrow'] - qry.rowcount
                    for r in range(0, rows_left):
                        # filler for no answers (blank cells)
                        hrow += "<TD>&nbsp;</TD><TD>&nbsp;</TD></TR>" 
            else:
                # filler for no answers (blank cells)
                hrow += "<TD>&nbsp;</TD><TD>&nbsp;</TD></TR>"
                n = 1 
            # adjust ROWSPAN for actual rows output (n) 
            hrow = hrow.replace('#?', str(n))
        html += hrow       
    # finish off HTML form - back link on bottom row right
    html += """<TR style="background-color: #ffffff"><TD COLSPAN='3'></TD>
        <TD align='right'><A href='/exec?m=sctn&u=%s'>Back to list</A></TD>
        </TR>""" % sid
    html += "</TBODY></TABLE>"     
    return html

def SCTN_tool(sid, environ=None):
    """
    Implements the SCTN tool.
    - sid is session ID, used to related linked calls and recall session data
    - environ contains GET, POST and other environment data per WSGI specs.
      If this is None (not supplied) the basic Data Tool page is displayed.
    """
    # retrieve POST data if applicable
    if environ is not None:
        #if environ.get('REQUEST_METHOD','') == 'POST':
        #    raise RuntimeError("Debug of SCTN_Tool")
        contentLength = int(environ.get('CONTENT_LENGTH', 0))
        requestBody = environ['wsgi.input'].read(contentLength)
        requestBody = requestBody.decode('utf-8')
        postFields = cgi.parse_qs(requestBody)
    else:
        # if no POST data, set postFields to None
        postFields = None     
    # HTML for debugging info - left blank if none
    debug = ""
    # HTML for the page header
    header = """
    <!DOCTYPE html>
    <HTML>
    <HEAD>
    <TITLE>SCTN Data Directory</TITLE>
    <LINK href="https://www.gc-dz.com/main.css" rel="stylesheet"  type="text/css">
    <SCRIPT src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></SCRIPT>
    <SCRIPT src="https://www.gc-dz.com/js/sctn.js"></SCRIPT>
    <META name="viewport" content="width=device-width, initial-scale=1.0">
    <META charset="UTF-8">
    </HEAD>
    <BODY class="sctn">
    <A name="Top" id="Top"></A>
    <!-- page heading organized as a table -->
    <TABLE class="sctn-list"><TR>
        <TD style="width: 100px"><IMG src="https://www.gc-dz.com/img/sctn_logo.png" border=0 height=48px width=98px></TD>
        <TD style="font-size: x-large; font-weight: bold; color: black; background-color: lightgray; text-align: center; vertical-align: middle;">
        Directory of Platforms and Attributes</TD>
    <TR><TD colspan="2" align="left">
        <BUTTON class="sctn" id="btnFilter" onclick="toggleButton(1)">Select</Button>
        <BUTTON class="sctn" id="btnData" onclick="toggleButton(2)">Columns</Button>
        <!-- <BUTTON class="sctn" id="btnStats" onclick="toggleButton(3)">Stats</Button> -->
    </TR></TABLE>    
    <FORM action="/exec?m=sctndd&u=%s" method="POST">
    """ % sid
    html = header
    # initialise database cursor
    qry = getCursor()
    # ---------------- Filter form layout and query modifers ----------------
    # form heading and table setup.  Form submit comes back here with SID set
    # and data in POST format.  Submit will hid the form.
    filter_form ="""
    <DIV class="sctn-panel" id="filterForm" style="display: none">
    <P class="sctn-data-panel">Show only platforms that include the selected features:</P>
    <TABLE style="table-layout: fixed; width: 100%%"><TR>
    """
    # add top row for filter-form table headings
    qry.execute("SELECT fid, question, tablehdr FROM sctn.features ORDER BY fid")
    hdrs = qry.fetchall()
    for hdr in hdrs:
        filter_form +="""
        <TD class="sctn-panel-hdr" title="%s" onclick="checkAll('Q%s')">%s</TD>
        """ % (hdr['question'], hdr['fid'], hdr['tablehdr'])
    # close row and open for second row
    filter_form += "</TR><TR>"
    # initialise dictionary of features (key) and flags (value)
    fidFlags = {}
    # this is a dictionary of column headings referenced by fid, used for the last table
    colhdrs ={}
    # work through column-wise
    for hdr in hdrs:        
        # add subcategories as checkboxes
        # get list of sub-category names
        fid = hdr['fid']
        colhdrs[fid] = hdr['tablehdr']
        qry.execute("SELECT fid, lvl, ftext, flag FROM sctn.flevels WHERE fid=%s", (fid,))
        levels = qry.fetchall()
        # add cell for this category
        filter_form += '<TD class="sctn-panel-sub">'
        # check if POST data present for this category (Question)
        if postFields is not None:
            qkey = "Q%s" % fid
            qflags = postFields.get(qkey,['0'])
        # add sub-category data
        for level in levels:
            # scan POST array for any matching flag value
            chk = ""
            if postFields is not None:
                for qflag in qflags:
                    f = int(qflag)
                    if f == level['flag']:
                        # mark checkbox as ticked
                        chk="checked"
                        # add flag to list for later query construction
                        if fid in fidFlags:
                            fidFlags[fid] += f
                        else:
                            fidFlags[fid] = f                            
                        break
            # create HTML for checkbox        
            filter_form += """<INPUT type="checkbox" name="Q%(FID)s" value="%(FLAG)s" %(CHK)s>
            %(FTEXT)s<BR>
            """ % {'FID': level['fid'], 'FLAG': level['flag'], 'FTEXT': level['ftext'], 'CHK': chk}
        # close table cell for this item
        filter_form += "</TD>"    
    # finish off form with OK and Cancel buttons
    filter_form += """
        </TR></TABLE>
        <P class="sctn-info">Click a column heading to select all.  
        &nbsp;Only platforms with a selected attribute will be listed.
        &nbsp;In the data table, columns with no selection will not be shown.</P>
        <P style="margin-left: 10px; margin-top: 10px; margin-bottom: 10px">
        <INPUT class="sctn-submit" type="submit" name="submit" value="Apply"></P>
        </DIV>
    """    
    html += filter_form
    # construct the query for filtering the list of platforms 
    # general exclusion for platforms with no data
    has_data = " platid IN (SELECT platid FROM sctn.sheets) "
    if len(fidFlags)>0:
        # first empty flag_check table
        qry.execute('TRUNCATE sctn.flag_check');
        # convert dictionary fidFlags into a text list for use with INSERT statement
        sqlValues =""
        comma=""
        for f in fidFlags:
            sqlValues += comma + "(%s,%s)" % (f, fidFlags[f])
            comma = ","
        sqltext = "INSERT INTO sctn.flag_check VALUES " + sqlValues
        qry.execute(sqltext)
        # where string to select platforms
        where = """ WHERE platid IN (SELECT DISTINCT platid FROM sctn.platform_flags AS t 
	    INNER JOIN sctn.flag_check AS f ON t.fid=f.fid AND (t.bit_or & f.flags)>0) AND %s
	    """ % has_data
    else:
	    # no filters - empty where string        
        where = " WHERE %s" % has_data
    # retrieve a list of platforms subject to the filter condition
    sqltext = """SELECT p.platid, p.platname, p.platurl, o.orgnm, o.orgurl 
    FROM sctn.platforms AS p INNER JOIN sctn.organs AS o on p.orgid=o.orgid %s ORDER BY 2 
    """ % where   
    qry.execute(sqltext) 
    platforms = {}
    alpha_order = []  # this retains orginal query order (alphabetical)
    if qry.rowcount>0:
        rows = qry.fetchall()     
        # convert retrieved list into a dictionary referenced by platform ID
        for row in rows:
            platforms[row['platid']] = {'platname': row['platname'], 'platurl': row['platurl'], \
                    'orgnm': row['orgnm'], 'orgurl': row['orgurl']}  
            alpha_order.append(row['platid'])                        
    # ---------- data form section : controls display of columns -------------  
    # HTML for heading section of form
    data_form ="""
    <DIV class="sctn-data-panel" id="dataForm" style="display: none">
    <P style="font-weight: bold; color: Teal;">Select columns to display:</P>
    """
    # generate list of checkboxes for column headers
    chk=""
    if postFields is not None and postFields.get('C0',['0'])[0] > '0':
        chk = "checked"
        # add C0 (organization) to column list for the output table
        if 0 not in fidFlags:
            # add key - data zero indicates this is a display rather than selection column
            fidFlags[0] = 0         
    data_form += """
    <P class="sctn-data-panel"><INPUT type="checkbox" name="C0" value="1" %s>
        &nbsp;&nbsp;Organization supporting the platform</P>
    """ % chk
    for hdr in hdrs:
        # check if POST data shows this column as ticked 
        fid = hdr['fid']
        chk = ""    
        if postFields is not None:
            # column will be displayed (checked) if checked on data form (C) or on Filter form (Q)
            ckey = "C%s" % fid
            qkey = "Q%s" % fid
            qflags = postFields.get(qkey,[])
            if postFields.get(ckey,['0'])[0] > '0' or len(qflags)>0:
                chk = "checked"
                # check if key already in column list, if not add it.
                if fid not in fidFlags:
                    # add key - data zero indicates this is a display rather than filter column
                    fidFlags[fid] = 0         
        # create HTML for checkbox and text      
        data_form += """<P class="sctn-data-panel"><INPUT type="checkbox" name="C%(FID)s" value="1" %(CHK)s>
        &nbsp;&nbsp;%(QUESTION)s</P>
        """ % {'FID': hdr['fid'], 'QUESTION': hdr['question'], 'CHK': chk}
    # HTML for bottom of form
    data_form += """
        <P class="sctn-info">Click columns to be displayed.</P>
        <P style="margin-left: 10px; margin-top: 10px; margin-bottom: 10px">
        <INPUT class="sctn-submit" type="submit" name="submit" value="Apply"></P>
        </DIV>
    """          
    html += data_form
    # --- list of platforms with selected rows (filter form) and columns (data form) ---
    # construct a list of applicable columns for use in the SQL command
    fidSet = ""
    if len(fidFlags)>0:
        fidSet = " AND v.fid IN (" 
        sep = ""
        for fid in fidFlags.keys():
            fidSet += sep + str(fid)
            sep = ","
        fidSet += ") "    
    # retrieve the row-column entries, indexed by platid (rows) and fid (columns)
    sqltext = """
    SELECT  v.platid, v.fid, L.ftext, L.flag FROM sctn.platform_flags AS v INNER JOIN sctn.flevels AS L 
    ON v.fid=L.fid WHERE v.bit_or & L.flag >0 %s ORDER BY 1, 2;
    """ % fidSet
    # initialise html for the table header, if there are rows to output
    #debug += "platforms type is : " + str(type(platforms)) + "\nlength " + str(len(platforms))
    #debug += "\n" + str(platforms)
    if len(platforms) > 0:
        html += "<TABLE class='sctn-table'><THEAD><TR><TD class=\"hdr\">Platform name</TD>"
        # create columns based on keys in fidFlags 
        fidKeys = list(fidFlags.keys())
        fidKeys.sort()
        for fid in fidKeys:
            if fid==0:
                html += "<TD class=\"hdr\">Organization</TD>"
            else:   
                html += "<TD class=\"hdr\">%s</TD>" % colhdrs[fid]
        # end of heading row        
        html += "</TR></THEAD><TBODY class='scroll'>"
        # platform counters and colours for row backgrounds
        np = 0
        bgnd = ['#ffffff','#f1f1f1']   # white, pale grey  
        # get set of rows to process
        qry.execute(sqltext)
        rows = qry.fetchall()
        # work through rows grouped by platforms
        for platid in alpha_order:
            if platid in platforms:
                # set row background color, increment platform counter afterwards
                # odd rows get bgnd[1], even rows are bgnd[0]
                bg = 'style="background-color: %s"' % bgnd[np % 2]
                np += 1
                # write platform name, url 
                platform = platforms[platid]
                html += """<TR><TD %(BG)s class="padded"><A class="sctn" href="%(URL)s" 
                style=\"font-weight: bold; text-decoration: underline\" target=_blank>%(NAME)s</TD>
                """ % {'URL': platform['platurl'], 'NAME': platform['platname'], 'BG': bg }
                # create columns based on keys in fidFlags dictionary
                for fid in fidKeys:
                    if fid==0:
                        # organization name - same format as platform name
                        html += '<TD %(BG)s class="padded"><A class="sctn" href="%(URL)s" target=_blank>%(NAME)s</TD>' \
                            % {'URL': platform['orgurl'], 'NAME': platform['orgnm'], 'BG': bg }
                    else:
                        # there may be several entries for each fid number  
                        html += "<TD %s class='padded'>" % bg      # start table cell
                        br = ""                     # initially no break before line
                        # scan data set for matching platform and feature id
                        for row in rows:
                            if row['fid'] == fid and row['platid']==platid:
                                if row['flag'] & fidFlags[fid]:
                                    # use bold text
                                    html += br + "<SPAN style=\"font-weight: bold\">" + row['ftext'] + "</SPAN>"
                                else:
                                    # normal text    
                                    html += br + row['ftext']
                                br = "<BR>"  # break before start of next line
                        html += "</TD>"   # end table cell
                # end of row                
                html += "</TR>"
        # end of table
        html += "</TBODY></TABLE>"
    else:
        # no data found that meets filer conditions
        html += "<P>-- No platforms match the filter conditions --</P>"    
    # set True to show Post fields in debug area
    if False:           
        debug += "\n\nPOST fields:\n"
        # list of POST fields
        for field in postFields.keys():
            # each field is represented as a list of 1 or more items
            for f in postFields[field]:
                debug += '%s = %s\n' % (field, f)      
    # add debugging info to foot of page, if present
    if debug > "":
        html += "<HR style=\"border-top: 5px solid red\"><BR><PRE>%s</PRE>" % debug
    # close off page and return
    html += "</FORM></BODY></HTML>"
    return html
    
def normal_reply(title, msg):
    template = "<p>%s</p>"   
    # substitute variable text in page template
    html = HTML_header(title)
    html += (template % msg)
    html += HTML_footer()
    # return HTML for page
    return html
    
def error_page(msg):
    # returns HTML for an error page, with message 'msg' in red bold
    template = '<p style="font-weight:bold; color: red">%s</p>'
    html = HTML_header("Data Manager Development Site")
    html += (template % msg)
    html += HTML_footer()
    # return HTML for page
    return html
    
def user_setup():
    # manages user registration and password resets via the email interface 
    # test of email module
    # Example:
    t = datetime.datetime.now()
    sendMail('d.alder@globalcanopy.org',
        'post@denisalder.com',
        'Email test',
        'Message from gc_dz.com at %s' % t.ctime() )


def sendMail(fro, to, subject, text, server="localhost"):
    msg = "From: %s\r\nTo: %s\r\nDate: %s\r\nSubject: %s\r\n\r\n%s" %  \
        (fro, to, formatdate(localtime=True), subject, text)
    smtp = SMTP()  
    smtp.connect()  
    smtp.sendmail(fro, to, msg)
    smtp.close()
    
def getCursor():
    # returns a database cursor for db 'trasepad'
    # get authorisation codes
    (a, b) = dbauth.dbauth()
    # open connection to db
    db = psycopg2.connect(Fernet(a).decrypt(b).decode('utf-8')) 
    db.autocommit
    # create PG cursor with dictionary keys for field names             
    cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return cur    
   
def session_id():
# return 16-char random session id, method after stackoverflow.com q# 2257441
    chars="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz"
    # make random draw of 16 characters
    qry = getCursor()
    found = True
    n=0
    while found:
        sid=""
        for i in range(16):
            sid += random.choice(chars)
        # check session ID is unique (query returns row count zero)    
        qry.execute("SELECT sessionid FROM gcdz.logins WHERE sessionid=%s",(sid,))
        found = qry.rowcount>0
        # check for looping error (not to lock up program)
        n+=1
        if n>10: 
            raise RuntimeError("Generated session ID '%s' problematic" % sid) 
    # supply unique session ID   
    return sid    

def sessionCheck(sid):
    # checks if session ID is an open session, and if so, returns user name
    qry = getCursor()
    qry.execute("""
            SELECT U.username FROM gcdz.logins as L INNER JOIN gcdz.users AS U 
            ON L.userid=U.userid WHERE L.sessionid=%s AND L.timeout IS NULL
        """,(sid,))
    if qry.rowcount > 0:
        # get username
        flds = qry.fetchone()
        usernm = flds['username']
    else:
        usernm = ""
    return usernm         

# when run from the command line for testing, does a simple compile check
if __name__ == '__main__':
    print('-- compiled OK --')

    

    
    
    
    
    