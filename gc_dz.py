"""
 ------------- gc_dz.py version 0.11 Feb 2019 ------------
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
# note that in development versions, /exec is substituted by /dev

# --- revision history ---
# v 0.07 25 Jan 2019  Adding code for revised login procedure via email link
# v 0.08 08 Feb 2019  Menu system access from email link, also remove obsolete code
# v 0.09 11 Feb 2019  Permit system finalised, landing page for F500
# v 0.10 18 Feb 2019  Trase/Neural Alpha name matching tools implemented
# v 0.11 22 Feb 2019  Forest 500 company listing/selection
# ------------------------
 
# CGI interface functions for Python
# plus other common utilities
import cgi, traceback, json
import os, sys, re, datetime
import random, string
from urllib.parse import urlparse
import unidecode

# Python modules for the email system and encryption
from smtplib import SMTP
from email.utils import formatdate
from cryptography.fernet import Fernet
import hashlib

# postgreSQL interface module
import psycopg2
import psycopg2.extras

# trase/Neural Alpha toolkit
from trasesdk import TraseCompaniesDB

# local modules
import dbauth

def application(environ, start_response):
    """
    Entry point called by Apache 2.4 via mod_wsgi, aliased as gc-dz.com/exec.
    Note that the returned string must be of type byte, in [] wrapper otherwise 
    you get a server error, with diagnostics only to Apache error log.  The routine
    basically decides whether this is an Ajax call, and if so invokes the Ajax handler,
    or a normal page, in which case it calls the page selector.  It is also the top-level
    error handler for the entire module, and returns an HTML-compatible trace-back
    for any Python errors.
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
        global homeURL, scriptnm  # *** this needs to be changed, globals inhibit modularization
        homeURL = '%s://%s' % (environ['REQUEST_SCHEME'], environ['SERVER_NAME']) 
        scriptnm =  environ['SCRIPT_NAME']  
        # check if this is an Ajax request.  If so, go to the Ajax handler
        if environ.get('HTTP_X_REQUESTED_WITH','') == 'XMLHttpRequest':
            html = ajaxHandler(environ)
        # otherwise continue processing as a GET/POST request    
        elif (environ['REMOTE_ADDR'] not in permaddr) and (environ['REQUEST_SCHEME'] != "https"):
            # originating IP address not on permitted list - show error page
            #raise RuntimeError("Blocked user!!" )
            html = error_page("Access not allowed (%s://%s )" % (environ['REQUEST_SCHEME'], environ['REMOTE_ADDR']), 1)
        else:
            # call main page    
            #raise RuntimeError("Testing!" )
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
    # mdl is module to be run; sid is session ID;  pflag is user's permissions (bitwise flags)
    (mdl, sid, pflag) = sessionStart(environ)
    if mdl == 'menu':
        # display menus - either active or disabled (grayed out) if not permitted for this user
        html = mainMenu(sid, environ, pflag)     
    elif mdl == 'cmatch':
        # company lists and name matching
        html = cmatch_tool(sid, environ)     
    elif mdl == 'f500':
        # company lists and name matching
        html = f500_main(sid, environ)     
    elif mdl == 'sctn':
        # SCTN survey system: latform list, platform detail, survey form
        html = SCTN_sys(sid, environ)
    elif mdl == 'sctndd':
        # SCTN data tool.  This is used by SCTN website.  Test any mods carefully! 
        html = SCTN_tool(sid, environ)
    elif mdl.find("<HTML>") >= 0:
        # html code has been returned, indicating an error page
        html = mdl    
    else:
        # unrecognised module
        html = error_page("Requested module '%s' is not available" % mdl)
    # return HTML for generated page to be displayed by server
    return html

def mainMenu(sid, environ, pflag):
    # displays the main menu - get the menu details
    # sid - session ID, environ - Apache/WSGI environment array, pflag - permit flag for user)
    qry = getCursor()
    qry.execute("SELECT id, menutext, module, permitflag, prtorder FROM gcdz.menus ORDER BY prtorder")
    if qry.rowcount<=0:
        # if no rows, then a program/database error has occurred
        raise RuntimeError("Menu system not found!!" )
    # make template for link URL    
    t_url = '%s://%s/%s?m=%%s&u=%%s' % (environ['REQUEST_SCHEME'], environ['SERVER_NAME'],environ['SCRIPT_NAME']) 
    # style for disabled rows
    styles = "<STYLE>li.gray {color: gray; }</STYLE>"
    # create HTML page
    html = HTML_header("Trasepad Menu", styles)
    rows = qry.fetchall()     
    # create list of menu items as links, which are disabled if permit not valid
    html += "<UL>"
    for row in rows:     
        # add module name and session ID to link
        url = t_url % (row['module'], sid)
        # check if permit flag matches user
        if (row['permitflag'] & pflag) or row['permitflag']==0:
            # write line of HTML with link
            html += "<LI><A href='%s'>%s</A>" % (url, row['menutext'])
        else:
            html += "<LI class='gray'>%s" % row['menutext']
     # end list and do page footer   
    html += "</UL>"
    html += HTML_footer()
    return html
    
def getPostFields(environ, getRequest=False):
    # returns the fields in the POST request.  They are returned as a dictionary
    # indexed by the NAME attribute from the HTML tag.  Each field is a sequence
    # as NAMEs may be repeated, so the first entry for field 'x' is postfields['x'][0]
    contentLength = int(environ.get('CONTENT_LENGTH', 0))
    requestBody = environ['wsgi.input'].read(contentLength)
    requestBody = requestBody.decode('utf-8')
    postFields = cgi.parse_qs(requestBody)
    # return both postFields and requestBody as a tuple if getRequest is true,
    # otherwise just the postFields
    if getRequest:
        return (postFields, requestBody)    
    else:
        return postFields    

def ajaxHandler(environ):
# process ajax requests
    try:
        # get post data 
        (postFields, requestBody) = getPostFields(environ, True)
        # get action ID and call approriate action based on that
        actid = postFields.get('action',['0'])[0]
        if actid=="login":
            # process login request
            emailaddr = postFields.get('emailaddr',['0'])[0]
            ip = environ['REMOTE_ADDR']
            html = user_login(emailaddr, ip)
            json_str = json.dumps({'html' : html})   
        elif actid=="f500.colist":
            # forest 500 company list matching 'cofind'
            cofind = postFields.get('cofind',['0'])[0]  
            sid = postFields.get('sid',['0'])[0]  
            html = f500_cofind(sid, cofind)
            json_str = json.dumps({'html' : html})   
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
    
def user_login(emailaddr, ip):
# checks an email address to see if a registered user.   If they are, sends
# a link to the menu page with a session ID.  If not, returns a message to
# the login page.  
    # check email address, get userid
    qry = getCursor()
    qry.execute("SELECT userid FROM gcdz.users WHERE email=%s", (emailaddr,))
    # if no match, user does not yet exist.
    if qry.rowcount<=0:
        # regexes for email format checks
        any_email = re.compile('[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,6}') # any email address
        gc_email =  re.compile('[a-zA-Z0-9.-_]+@(sei|globalcanopy)\.org')  # GC or SEI domain
        # check if text is in valid email address format
        if any_email.match(emailaddr) is None:
            # bold red message if not in email format
            html='<P style="font-weight: bold; color: red">Please enter a valid email address!</P>'
            return html
        elif gc_email.match(emailaddr) is None:
            # see if this looks like a valid email address with either GC or SEI domain
            html="""<P style="font-weight: bold; color: red"> Only GC and SEI staff or 
            consultants can register to access this site.</P>  
            <P>If you do not have an email with a @globalcanopy.org or @sei.org domain, please
            email your details and justification for access to the Global Canopy Data Manager 
            (<A href="mailto:d.alder@globalcanopy.org" target = _blank>d.alder@globalcanopy.org</A>).</P>
            """
            return html
        else:
            # valid SE/GC email but not yet in users table - add them
            username = emailaddr.split("@")[0]
            # get next vailable userid
            qry.execute("SELECT max(userid) +1 as nextid FROM gcdz.users");
            nextid = qry.fetchone()['nextid']
            qry.execute("INSERT INTO gcdz.users (userid, username, email, permit) VALUES(%s, %s, %s, 3)" , (nextid, username, emailaddr))
    # close any open sessions for this user
    flds = qry.fetchone()
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
    # provide menu link and session ID by email
    mail_from = "noreply@gc-dz.com"    
    mail_to = emailaddr
    subject = "Login link for gc-dz.com"
    text ="""
    Please use the link below to access the Global Canopy Data portal.
    
    https://www.gc-dz.com/exec?m=menu&u=%s
    
    This link is only valid from this location and expires in 48 hours.
    Use the login page to generate a new link as necessary.  
    You will automatically be referred there if the link is invalid.  
    Please contact me if any queries or problems.  
    
    Denis Alder
    Data Manager, Global Canopy
    d.alder@globalcanopy.org 
    """ % sid
    sendMail(mail_from, mail_to, subject, text)
    # return text for home page
    html="""<P>An email has been sent to %s with a link to portal.  Please use this link 
    to enter the site.  It will expire in 48 hours and will only work from this location.</P>  
    """  % emailaddr
    # return text for home page 'reply_div' 
    return html
    
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
    
def normal_reply(title, msg):
    template = "<p>%s</p>"   
    # substitute variable text in page template
    html = HTML_header(title)
    html += (template % msg)
    html += HTML_footer()
    # return HTML for page
    return html
    
def error_page(msg, lgin=0):
    # returns HTML for an error page, with message 'msg' in red bold
    template = '<p style="font-weight:bold; color: red">%s</p>'
    html = HTML_header("Data Manager Development Site")
    html += (template % msg)
    if lgin==0:
        # standard 'Back link' on page footer
        html += HTML_footer()
    else:
        # 'Login' link for bigger errors
        html += """
        <p>&nbsp;</p>
        <p><A href="https://www.gc-dz.com">Login</A></p>
        </BODY>
        </HTML>    
        """               
    # return HTML for page
    return html
    
def sendMail(fro, to, subject, text, server="localhost"):
    msg = "From: %s\r\nTo: %s\r\nDate: %s\r\nSubject: %s\r\n\r\n%s" %  \
        (fro, to, formatdate(localtime=True), subject, text)
    smtp = SMTP()  
    smtp.connect()  
    smtp.sendmail(fro, to, msg)
    smtp.close()
    
def getCursor():
    # returns a database cursor for db 'trasepad'
    # get a connection object for trasepad
    db = dbauth.dbconn()
    # make sure autocommit turned on
    db.autocommit = True
    # create PG cursor with dictionary keys for field names             
    cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return cur    
   
def session_id():
    # return unique 16-char random session id
    # basic method after stackoverflow.com q# 2257441
    # elaborated to ensure unique ID
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

def sessionStart(environ):
    # check session ID in URL.  Returns  a module name if valid,
    # otherwise HTML text for the error page with a message.
    #
    # Function checks session ID exists, is open, and from current IP.
    # It closes any session more than 48 hours old.
    # It adds a record in the stats table for the module accessed.
    #
    params = cgi.parse_qs(environ['QUERY_STRING']) 
    ip = environ['REMOTE_ADDR'] 
    # check u parameter for session ID
    sid = params.get('u', ['0'])[0]
    mdl = params.get('m', ['0'])[0]
    # html will be set only if an error condition occurs, otherwise it remains empty
    # permissions also default to zero until otherwise set
    html = "" 
    pflag = 0
    # valid session IDs must be 16 characters a-zA-Z0-9
    test_ptn =  re.compile('[a-zA-Z0-9\_]{16,16}')
    if test_ptn.match(sid) is None:
        # badly formed session ID
        html = error_page("Invalid parameters - Please log in again.", 1)
        return (html, sid, pflag)
    elif sid=='auto_login__sctn' and mdl=='sctndd':
        # log temporary user for stats
        sid = logTempUser(sid, ip)
        return (mdl, sid, pflag)  
    # session ID format is OK - now look up to see if a valid login
    qry = getCursor()
    # close any sessions more than 2 days old
    qry.execute("""UPDATE gcdz.logins SET timeout=NOW() WHERE timeout IS NULL 
        AND NOW()-timeout > '2 days' """ , {'SID': sid, 'IP': ip})
    # if this session was started from a different IP, close it
    qry.execute("""UPDATE gcdz.logins SET timeout=NOW() WHERE sessionid=%(SID)s AND timeout IS NULL 
        AND ipaddr <> %(IP)s""" , {'SID': sid, 'IP': ip})        
    # search for an open session    
    qry.execute("""
        SELECT U.userid, U.permit, L.timeon, L.ipaddr FROM gcdz.users AS U INNER JOIN gcdz.logins AS L
        ON U.userid=L.userid WHERE sessionid=%(SID)s AND timeout IS NULL
        """, {'SID': sid})
    # if no match, session does not exist or has timed out
    if qry.rowcount<=0:
        html = error_page("Session ID has expired (limit 48 hours) or was from a different network - Please login again", 1)
        return (html, sid, pflag)
    # at this point session has been validated.  Retrieve permit flags for this user        
    flds = qry.fetchone()
    pflag = flds['permit']
    if pflag is None:
        pflag=0
    # skip permissions check and timeon for the 'menu' module
    if mdl != 'menu':
        # see if allowed to access this module 
        qry.execute("SELECT id FROM gcdz.menus WHERE module=%s AND ((permitflag & %s)>0 OR permitflag=0)", (mdl, pflag))
        if qry.rowcount<=0:
            html = error_page("This Session ID (%s) does not have permission to access module '%s'" % (sid, mdl), 1)
            return (html, sid, pflag)
        # add session ID and module name to stats table
        qry.execute("INSERT INTO gcdz.stats (sessionid, module, timein) VALUES (%s, %s, NOW())", (sid, mdl))
    return (mdl, sid, pflag)

def sessionCheck(sid):
    # checks if session ID is an open session, and if so, returns user name
    # **** this is obsolete, retained for compatibility for time being ****
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

def SCTN_sys(sid, environ): 
    # implements the SCTN meta-database system
    # set page header with javascript libraries, debug messages (empty)
    jslib = """
    <SCRIPT src='js/sctn.js'></SCRIPT>
    <SCRIPT src='js/cookies.js'></SCRIPT>
    """
    html = HTML_header("SCTN Meta-database Tool", extras=jslib)
    debug = ""
    # get post data 
    postFields = getPostFields(environ)
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
    <FORM action="%s?m=sctn&u=%s" method="POST" id="listform">
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
    global scriptnm
    html = hdr % (scriptnm,sid)
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
    global scriptnm
    html = """
    <TABLE style="width: 1000px;">
        <TR><TD>Survey details for <B>%(NAME)s</B></TD>
            <TD><A href="%(URL)s" target=_blank>%(URL)s</A></TD>
            <TD align="right"><A href="%(SCRIPT)s?m=sctn&u=%(SID)s">Back to list</A></TD></TR>
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
    """ % {'NAME': pname, 'URL': purl, 'SID': sid, 'SCRIPT': scriptnm}
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
    global scriptnm
    html += """<TR style="background-color: #ffffff"><TD COLSPAN='3'></TD>
        <TD align='right'><A href='%s?m=sctn&u=%s'>Back to list</A></TD>
        </TR>""" % (scriptnm, sid)
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
        postFields = getPostFields(environ)
    else:
        # no post data 
        postFields = None
    # set default columns if nothing selected
    if postFields is None or len(postFields)==0:    
        # set postFields to some default columns
        postFields = {'C1': ['1'],'C3': ['1'],'C5': ['1'],'C6': ['1'],'C8': ['1'],'C10': ['1']}   
    # HTML for debugging info - left blank if none
    debug = ""
    global scriptnm   # *** this needs to be changed, globals inhibit modularization
    # HTML for the page header
    header = """
    <!DOCTYPE html>
    <HTML>
    <HEAD>
    <TITLE>SCTN Directory</TITLE>
    <LINK href="https://www.gc-dz.com/sctn.css" rel="stylesheet"  type="text/css">
    <SCRIPT src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></SCRIPT>
    <SCRIPT src="https://www.gc-dz.com/js/sctn.js"></SCRIPT>
    <META name="viewport" content="width=device-width, initial-scale=1.0">
    <META charset="UTF-8">
    </HEAD>
    <BODY onload="init()">
    <A name=Top id=Top></A>
    <!-- page heading organized as a table -->
    <TABLE><TR>
        <TD class=narrow><IMG src="https://www.gc-dz.com/img/sctn_logo.png" border=0 height=48px width=98px></TD>
        <TD align=center><H1>Directory</H1><P>Supply Chain Information Platforms</P></TD>
    </TR>    
    <TR><TD colspan=2><HR style="color: #f4fee7"></TD></TR>
    <TR><TD colspan=2 align=center>
        <BUTTON id=btnFilter onclick="toggleButton(1)">Filter</BUTTON><SMALL>&nbsp;&nbsp;Filter list by selected platform features</SMALL>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        <BUTTON id=btnData onclick="toggleButton(2)">Columns</BUTTON><SMALL>&nbsp;&nbsp;Select feature columns to include in table</SMALL>
        <!-- <BUTTON class="sctn" id="btnStats" onclick="toggleButton(3)">Stats</Button> -->
    </TR></TABLE>    
    <FORM action="%s?m=sctndd&u=%s" method=POST>
    """ % (scriptnm, sid)
    html = header
    # initialise database cursor
    qry = getCursor()
    # ---------------- Filter form layout and query modifers ----------------
    # form heading and table setup.  Form submit comes back here with SID set
    # and data in POST format.  Submit will hid the form.
    filter_form ="""
    <DIV class=sctn-panel id=filterForm>
    <P class=sctn-data-panel>Show only platforms that include the selected features:</P>
    <TABLE class=sctn-data-panel><TR>
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
        qry.execute("SELECT fid, lvl, ftext, flag FROM sctn.flevels WHERE fid=%s ORDER BY lvl", (fid,))
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
        <P class=button-spacer><INPUT class=sctn-submit type="submit" name="submit" value="Apply">
        </P></DIV>
    """    
    html += filter_form
    # construct the query for filtering the list of platforms 
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
        #debug += "\n\nQuery at line %s:\n%s" % (773, sqltext)
        qry.execute(sqltext)
        # where string to select platforms
        where = """ WHERE platid IN (SELECT platid FROM (SELECT platid, count(*) as nr FROM sctn.platform_flags AS t 
	    INNER JOIN sctn.flag_check AS f ON t.fid=f.fid AND (t.bit_or & f.flags)>0  GROUP BY 1 
	    HAVING count(*)>=(SELECT count(*) AS nr FROM sctn.flag_check)) AS x) 
	    AND  platid IN (SELECT platid FROM sctn.sheets)
	    """ 
    else:
	    # no filters - but only list platforms which have data      
        where = " WHERE platid IN (SELECT platid FROM sctn.sheets)"
    # retrieve a list of platforms subject to the filter condition
    sqltext = """SELECT p.platid, p.platname, p.platurl, o.orgnm, o.orgurl 
    FROM sctn.platforms AS p INNER JOIN sctn.organs AS o on p.orgid=o.orgid %s ORDER BY 2 
    """ % where   
    #debug += "\n\nQuery at line %s:\n%s" % (785, sqltext)
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
    # ---------- columns form section : controls columns to be displayed -------------  
    # HTML for heading section of form
    data_form ="""
    <DIV class="sctn-data-panel" id="dataForm" style="display: none">
    <P style="font-weight: bold; color: #466c13;">Select columns to display:</P>
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
    ON v.fid=L.fid WHERE v.bit_or & L.flag >0 %s ORDER BY 1, 2, 4;
    """ % fidSet
    #debug += "\n\nQuery at line %s:\n%s" % (856, sqltext)
    # initialise html for the table header, if there are rows to output
    #debug += "platforms type is : " + str(type(platforms)) + "\nlength " + str(len(platforms))
    #debug += "\n" + str(platforms)
    if len(platforms) > 0:
        html += "<TABLE class='sctn-table'><THEAD><TR><TD>Platform name</TD>"
        # create columns based on keys in fidFlags 
        fidKeys = list(fidFlags.keys())
        fidKeys.sort()
        for fid in fidKeys:
            if fid==0:
                html += "<TD>Organization</TD>"
            else:   
                html += "<TD>%s</TD>" % colhdrs[fid]
        # end of heading row        
        html += "</TR></THEAD><TBODY>"
        # platform counters and colours for row backgrounds
        np = 0
        bgnd = ['plainbg','greenbg']   # white, pale green  
        # get set of rows to process
        qry.execute(sqltext)
        rows = qry.fetchall()
        # work through rows grouped by platforms
        for platid in alpha_order:
            if platid in platforms:
                # write platform name, url 
                platform = platforms[platid]
                html += """<TR><TD><A class="sctn" href="%(URL)s" 
                target=_blank>%(NAME)s</A></TD>
                """ % {'URL': platform['platurl'], 'NAME': platform['platname']}
                # create columns based on keys in fidFlags dictionary
                for fid in fidKeys:
                    if fid==0:
                        # organization name - same format as platform name
                        html += '<TD><A class="sctn" href="%(URL)s" target=_blank>%(NAME)s</A></TD>' \
                            % {'URL': platform['orgurl'], 'NAME': platform['orgnm']}
                    else:
                        # there may be several entries for each fid number  
                        html += "<TD>"                # start table cell
                        br = ""                       # initially no break before line
                        # scan data set for matching platform and feature id
                        for row in rows:
                            if row['fid'] == fid and row['platid']==platid:
                                if row['flag'] & fidFlags[fid]:
                                    # use bold text
                                    html += br + "<B>" + row['ftext'] + "</B>"
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
    
def f500_main(sid, environ):
    # landing page for F500 tool, also handles GET/POSTs to m=f500 in URI
    # get POST data if any and the name of the calling app (dev or exec)
    postFields = getPostFields(environ)
   # set default POST values if none given
    if len(postFields)==0:    
        # set postFields to some default columns
        postFields = {'Mode': ['None']}   
    # HTML for debugging info - left blank if none
    debug = ""
    # name of calling script (exec or dev)
    scriptnm =  environ['SCRIPT_NAME']  
    # HTML for the page header - always visible
    header = """
    <!DOCTYPE html>
    <HTML>
    <HEAD>
    <TITLE>Trasepad : Forest 500 data</TITLE>
    <LINK href="https://www.gc-dz.com/css/f500.css" rel="stylesheet"  type="text/css">
    <SCRIPT src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></SCRIPT>
    <SCRIPT src="https://www.gc-dz.com/js/f500.js"></SCRIPT>
    <META name="viewport" content="width=device-width, initial-scale=1.0">
    <META charset="UTF-8">
    </HEAD>
    <BODY onload="init()">
    <A name=Top id=Top></A>
    <!-- page heading organized as a table -->
    <TABLE><TR>
        <TD class=narrow><IMG src="https://www.gc-dz.com/img/gc.jpg" border=0 height=48px width=182px></TD>
        <TD align=left ><H1>Forest 500 Companies and Assessments</H1></TD>
    </TR> 
    </TABLE>   
    <HR style="border-width: 5px; width: 1000px; margin-left: 0;">
    """
    html = header
    # Select rest of page layout depending on last Mode value 
    mode = postFields['Mode'][0]
    if mode=="None":
        # first entry to the page (no 'Mode' POSTed) - display company selection tool
        # this works via ajax.  See ajaxHandler(), f500_ajax()
        html += """
        <FORM action="%(SCRIPT)s?m=f500&u=%(SID)s" method=POST>
        <INPUT type=hidden name=Mode value='Company'>
        <INPUT type=hidden id=scriptnm value=%(SCRIPT)s>
        <INPUT type=hidden id=session-id value=%(SID)s>
        <INPUT type=hidden name=coid id=coid value=0>
        <TABLE class=colist><TR>
        <TD id=cofind class=cofind>
        Company name:<BR>
        <INPUT type=text id=cofind-text onkeyup='updateColist()' onclick='updateColist()'>
        <BR>Type at least three letters from any part of
        the company name that uniquely identifies it.  
        Regular Expressions can be used for variable
         text.<BR>&nbsp;<BR>
         Select the required company from the list to see further details.
        </TD><TD id=colist>
        &nbsp;
        </TD></TR></TABLE>
        """ % {'SCRIPT': scriptnm, 'SID': sid}
    elif mode=="Company":
        # first retrieve required detail from 'companies' table
        coid = postFields['coid'][0]
        qry = getCursor()
        qry.execute("SELECT * FROM f500.companies WHERE coid=%s", (coid,))
        # check coid is OK.  If no record retrieved dispay warning, otherwise show form with details
        if qry.rowcount == 0:
            html += """<P class=error>No company selected!</P>
                <BR><INPUT type=button onclick="window.history.back()" value="Cancel">
                """
        elif qry.rowcount > 1:
            html += """<P class=error>Database error: Company ID %s returned %s records from Companies table</P>
                <BR><INPUT type=button onclick="window.history.back()" value="Cancel">
                """ % (coid, qry.rowcount)
        else:
            co = qry.fetchone()
            role = 'Financial Institution' if co['cocat']=='FI' else co['codetails']
            # add section for company header form to HTML page
            html += """
            <FORM action="%(SCRIPT)s?m=f500&u=%(SID)s" method=POST>
            <INPUT type=hidden name=Mode value='CoUpdate'>
            <H2>%(CONAME)s</H2>
            <INPUT type=button onclick="window.history.back()" value="Back to list">
            <BR clear=all>
            """ % {'SCRIPT': scriptnm, 'SID': sid, 'CONAME': co['cogroup']}
            # test of floating datafield
            html += "<BR>"
            html += HTML_datafield("Group", "cogroup", co['cogroup'])
            html += HTML_datafield("Subsidiary", "cosub", co['cosubsid'])
            html += HTML_datafield("HQ", "cojur", co['cohq'])
            html += HTML_datafield("Role/Activities", "corol", role)
            html += "<BR clear=all>"
            html += HTML_xlco_search(co['cosubsid'])
            html += "</FORM>"
            debug += '%s = %s<BR>' % ('Name fragments', str(name_frags(co['cosubsid'])))
         
            # get a dictionary of years and file IDs eg, {'2014':30, '2015': 401} etc        
            #yrass = assFinder([co['cogroup'], co['cosubsid']])
            # create HTML for this
            
    # ---- debugging information - list of POST fields ------
    debug += "POST fields:<BR>"
    # trap sensibly any errors loopong through list of lists
    try:
        # list of POST fields
        for field in postFields.keys():
            # each field is represented as a list of 1 or more items
            for f in postFields[field]:
                debug += '%s = %s<BR>' % (field, f) 
    except Exception as e:
        # if error occurs in above, just show traceback
        debug += traceback.format_exc()
    # -------------------------------------------------------
    html += HTML_footer(debug, environ)
    return html

def HTML_xlco_search(coname):
    # search for a name from the 'Companies' list and returns matching files from the 'xlcohdr' table
    # in an HTML format as part of a form, together with assessment years
    # (1) tokenise 'coname' into a sequence of name fragments
    cofrags = name_frags(coname)
    # (2) retrieve xlcohdr table
    qry = getCursor()
    qry.execute("""SELECT x.flid, x.coname, f.flname, d.cotype, d.ayear FROM f500.xlcohdr AS x 
        INNER JOIN f500.filelist AS f ON x.flid=f.flid
        INNER JOIN f500.dirtree AS d ON d.dtid=f.dtid ORDER BY 1""")
    # convert query results into a list
    colist = qry.fetchall()
    # initialise the list of matches
    comatch = []
    # scan the list of names looking for matches
    for co in colist:
        # convert company name in list into fragments
        xfrags = name_frags(co['coname'])
        # if the names are 'equivalent', add the details to the list of matches
        if name_frags_equiv(cofrags, xfrags):
            comatch.append(co)
    # create an HTML table with the data
    # --- this will be enhanced later ---
    html = "<TABLE class=cohdr><TR><TH>Year</TH><TH>Company Name</TH><TH>File ID</TH><TH>File</TH></TR>"
    for co in comatch:
        html += "<TR><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD></TR>" \
            % (co['ayear'], co['coname'], co['flid'], co['flname'])
    # finish off table
    html += "</TABLE>"
    return html        

def name_frags(name):
    # splits a name into standardised fragments and returns them as a sequence
    nm = name.lower()                           # convert to lower case
    nm = re.sub('\.xls[x|m]\s*$','', nm)        # remove .xlsx or .xlsm at end of string
    nm = unidecode.unidecode(nm)                # clean up accented characters
    nm = re.sub('[^\sa-z]', ' ', nm)             # remove anything else that is not a letter, whitespace or hyphen  
    nm = re.sub(r'(?<![a-z])([a-z])\s(?![a-z]{2})', r'\1', nm)     # merge any single letters together
    #list of words that are to be removed
    stopwords = ['ltd', 'co', 'group', 'sa', 'inc', '&', 'holdings', 'and', 'corp', 'pt', 'international', 'de', \
                'the', 'corporation', 'y', 'plc', 'of', 'limited', 'grupo', 'ag', 'as', 'j', 'ooo', 'company', 'holding', 'i', 'gmbh']
    for sw in stopwords:
        rgx = "(^|[ ])%s($|[ ])" % sw           
        nm = re.sub(rgx,' ', nm)                # filter out unwanted words 
    nm = re.split('\s+', nm)                    # split at word boundaries
    return nm

def name_frags_equiv(frags1, frags2):
    # returns True if the two lists share tokens in the same order
    # if there is more than 1 token, than 1 less than the total can match
    # to give equivalence.  If only one, it must match
    if len(frags1)> len(frags2):
        #always have frags 1 as the shorter of the two if they differ
        frags2, frags1 = frags1, frags2
    hits = 0    
    for i in range(len(frags2)):       
        for j in range(i, len(frags1)):
            if frags1[i]==frags2[j]:
                # token in frags1 matches that in frags2, so count it and exit inner loop
                hits += 1
                break
    # equivalence is 1 hit if shorter has 1 word, 2 if 2-3 words, 3 if 4 or more
    #equiv = False
    #if len(frags1)==1:
    #    equiv = hits >= 1
    #elif len(frags1)<=3:
    #    equiv = hits >= 2
    #elif len(frags1)>3:
    #    equiv = hits >= 3
    equiv = hits == len(frags1)
    return equiv    

def HTML_datafield(hdr, name, text, minsize=10, pxc=1):
    # returns a floating DIV with heading text 'hdr' and input field 'name' with value 'text'
    # size is estimated dynamically as larger of hdr, text, minsize, assuming 'pxc' per char
    sz = max(len(hdr), len(text), minsize)* pxc
    html = """
    <DIV class=datafield style="width: %(WIDTH)sch">
    <P>%(HDR)s</P>
    <INPUT type=text name=%(NAME)s value="%(TEXT)s">
    </DIV>
    """ % {'WIDTH': sz, 'HDR': hdr, 'NAME': name, 'TEXT': text}
    return html
    
def f500_cofind(sid, cofind):
    # simple search of 'companies' for a matching string
    qry = getCursor()
    qry.execute("SELECT coid, cosubsid, coyear FROM f500.companies WHERE cosubsid ~* %s ORDER BY 2, 3", (cofind,))
    # only show first m, or all if less than m matches
    m = 20
    if qry.rowcount>m:
        flds = qry.fetchmany(m)
    elif qry.rowcount>0:
        flds = qry.fetchall()
    else:
        html = "<P>[No matches found]</P>"
        return html
    # list companies found  
    html = "<UL>"  
    for co in flds:
        html += "<LI class=colink onclick='fetchCompany(%s)'>%s [%s]" % (co['coid'], co['cosubsid'], co['coyear'])
    html += "</UL>"  
    return html        

def yrass(names):
    # get a dictionary of years and file IDs eg, {'2014':30, '2015': 401} etc        
    pass

def cmatch_tool(sid, environ):
    # implements company listing and name matching tools
    # the code is borrowed and adapted from SCTN tool, it shares similar style and mode of action
    # get POST data if any and the name of the calling app (dev or exec)
    postFields = getPostFields(environ)
    # set default POST values if none given
    if len(postFields)==0:    
        # set postFields to some default columns
        postFields = {'Mode': ['None']}   
    # HTML for debugging info - left blank if none
    debug = ""
    # name of calling script (exec or dev)
    scriptnm =  environ['SCRIPT_NAME']  
    # HTML for the page header and mode buttons
    header = """
    <!DOCTYPE html>
    <HTML>
    <HEAD>
    <TITLE>Trasepad : Company Name Matching, Selection and Listing Tools</TITLE>
    <LINK href="https://www.gc-dz.com/css/cmatch.css" rel="stylesheet"  type="text/css">
    <SCRIPT src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></SCRIPT>
    <SCRIPT src="https://www.gc-dz.com/js/cmatch.js"></SCRIPT>
    <META name="viewport" content="width=device-width, initial-scale=1.0">
    <META charset="UTF-8">
    </HEAD>
    <BODY onload="init()">
    <A name=Top id=Top></A>
    <!-- page heading organized as a table -->
    <FORM action="%s?m=cmatch&u=%s" method=POST>
    <TABLE><TR>
        <TD class=narrow><IMG src="https://www.gc-dz.com/img/gc.jpg" border=0 height=48px width=182px></TD>
        <TD align=left ><H1>Company Name Matching, Selection and Listing Tools</H1></TD>
    </TR> 
    </TABLE>   
    <HR style="border-width: 5px; width: 1000px; margin-left: 0;">
    <P>
    <INPUT type="submit" name="Mode" value="Match">&nbsp;&nbsp;<SMALL>Find matches for full or partial names</SMALL>
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
    <INPUT type="submit" name="Mode" value="Filter">&nbsp;&nbsp;<SMALL>Select companies by attribute</SMALL>
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
    <INPUT type="submit" name="Mode" value="List">&nbsp;&nbsp;<SMALL>List selection with chosen details</SMALL>
    </P>
    </FORM>    
    """ % (scriptnm, sid)
    html = header
    mode = postFields['Mode'][0]
    qry = getCursor()
    if mode=="Match":
        # segment of html for name matching tool
        # set default data values for controls
        dd = {'CLIST': '', 'TRASE_CHK': 'checked', 'F500_CHK': '', 'SIMPLE_CHK': 'checked','FULL_CHK': ''}    
        # check if the 'GO' button has been clicked, and if so replace defaults with passed data
        if postFields.get('Search',[''])[0] == 'Go':    
            # save current settings as default values
            dd['CLIST'] = postFields.get('clist',[''])[0]
            dd['TRASE_CHK'] = 'checked' if postFields['search_opt'][0] == 'trase' else ''
            dd['F500_CHK'] = 'checked' if postFields['search_opt'][0] == 'f500' else ''
            dd['SIMPLE_CHK'] = 'checked' if postFields['output_opt'][0] == 'simple' else ''
            dd['FULL_CHK'] = 'checked' if postFields['output_opt'][0] == 'full' else ''
            qry.execute("""INSERT INTO gcdz.def_data (sessionid, ddtag, ddinfo) VALUES (%(SID)s, 'cmatch', %(DD)s)   
                ON CONFLICT ON CONSTRAINT defdata_pk DO UPDATE SET ddinfo= %(DD)s 
                """, {'SID': sid, 'DD': json.dumps(dd)}) 
            # get list of names to check, method, and output format
            nmlist =  re.split(r'\r\n', dd['CLIST'])
            chktype =  postFields['search_opt'][0]
            outfmt =  postFields['output_opt'][0]
            # results of name check are returned as HTML
            html += nameCheck(nmlist, chktype, outfmt)
        else:             
            # retrieve default values if GO was not clicked (as on first entry to this tool)
            qry.execute("SELECT sessionid AS sid, ddtag, ddinfo FROM gcdz.def_data WHERE sessionid=%s AND ddtag='cmatch'", (sid,))
            # defaults found, retrieve them to local variables
            if qry.rowcount>0:
                flds = qry.fetchone()
                dd = flds['ddinfo']
            # HTML for displayed form        
            html += """
            <FORM action="%(APP)s?m=cmatch&u=%(SID)s" method=POST>
            <BR>&nbsp;<BR>
            <TABLE class=match><TR style="vertical-align: top">
                <TD>
                    <TEXTAREA rows=14, cols=70 name="clist">%(CLIST)s</TEXTAREA>
                </TD><TD style="font-size: small; padding-left: 30px;">
                    Type or paste a list of names or name fragments.  For quick lookup, type a single name and click the GO button.
                    <BR>&nbsp;<BR>
                    <U>Method</U><BR>
                    <INPUT type="radio" value="trase" name="search_opt" %(TRASE_CHK)s>&nbsp;&nbsp;Use trase/Neural Alpha toolkit<BR>
                    <INPUT type="radio" value="f500" name="search_opt" %(F500_CHK)s>&nbsp;&nbsp;Search F500 companies
                    <BR>&nbsp;<BR>
                    <U>Output</U><BR>
                    <INPUT type="radio" value="simple" name="output_opt" %(SIMPLE_CHK)s>&nbsp;&nbsp;Simple output (1 line per company)<BR>
                    <INPUT type="radio" value="full" name="output_opt" %(FULL_CHK)s>&nbsp;&nbsp;All available detail (multiline per company)
                    <BR>&nbsp;<BR>
                    <INPUT type="submit" name="Search" value="Go">
                </TD><TD>
            </TR></TABLE>
            <INPUT type="hidden" name="Mode" value="Match">
            </FORM>        
            """ % {'APP': scriptnm, 'SID': sid, 'CLIST': dd['CLIST'], 'TRASE_CHK': dd['TRASE_CHK'], 'F500_CHK': dd['F500_CHK'], \
                'SIMPLE_CHK': dd['SIMPLE_CHK'],'FULL_CHK': dd['FULL_CHK']}
    elif mode=="Filter":
        pass
    elif mode=="List":
        pass    
    html +=""
    # ---- debugging information - list of POST fields ------
    debug += "POST fields:<BR>"
    # trap sensibly any errors loopong through list of lists
    try:
        # list of POST fields
        for field in postFields.keys():
            # each field is represented as a list of 1 or more items
            for f in postFields[field]:
                debug += '%s = %s<BR>' % (field, f) 
    except Exception as e:
        # if error occurs in above, just show traceback
        debug += traceback.format_exc()
    # -------------------------------------------------------
    html += HTML_footer(debug, environ)
    return html

def nameCheck(nmlist, chktype, outfmt):
# undertakes a name search and returns information on any matches in HTML format
# nmlist - a list of names to be checked
# chktype - 'trase' for trase/nural alpha engine, 'f500' for my own recipe
# outfmt - output format, eith 'simple' (one line per company) or 'full'
# -- first draft of routine simply explores what the trase/NA SDK gives back
    # trase/NA API
    api = TraseCompaniesDB("iiYrZqe3eX4fP5KD7RXIC5YlLehjaV7a5mj7dbfj")
    # set table heading HTML for output
    html = """<TABLE class=trase-full-list>
                <TR><TH rowspan=2>Search term</TH>
                <TH rowspan=2>Trase name</TH>
                <TH rowspan=2>Juris.</TH>                
                <TH rowspan=2>Alternate names</TH>
                <TH colspan=2>Identifiers</TH></TR>
                <TR><TD class=tflh>Type</TD><TD class=tflh>ID</TD></TR>
                """ 
    for nm in nmlist:
        #skip any names that are not words 
        if re.search('\w+', nm) is None:
            continue
        matches = api.match(nm, threshold=0.9, balance=0.5)
        if len(matches) == 0:
            # no match found
            html += "<TR><TD>%s</TD><TD colspan=5>No matches found</TD></TR>" % nm
        else:
            # one or more matches
            nr = len(matches)
            # html for left hand column
            html += "<TR><TD rowspan=%s>%s</TD>" % (nr, nm)
            n=0
            for item in matches:
                # new row if more than first match
                #html += "<TD colspan=5><PRE>%s</PRE></TD></TR>" % ('match' in item)   
                #continue        
                co = item['match']
                if n>0:
                    html += "<TR>"
                # trase name
                html += "<TD>%s</TD>" % co['name']
                # jurisdiction - either country field if present or 
                if 'country' in co:
                    html += "<TD>%s</TD>" % co['country']
                elif 'open-corporates' in co:
                    html += "<TD>%s</TD>" % str(re.findall('http[s]*://opencorporates\.com/companies/([A-Za-z_]+)/',co['open-corporates'])[0]).upper()  
                else:    
                    html += "<TD>-</TD>"    
                # alternate names
                altnames = co['label']
                html += "<TD>"
                sep=""
                for altnm in altnames:
                    html += sep + altnm
                    sep = "<BR>"
                html += "</TD>" 
                # identifiers for trase, LEI, Permid, Open-Corporates
                td = ["", ""]     # td[0] has ID types, td[1] as ID values
                nl = ""          # newline at start (none for 1st line, then <BR>)
                if 'id' in co:
                    td[0] += nl + "trase ID"
                    td[1] += nl + co['id']
                    nl = "<BR>"
                if 'lei' in co:
                    td[0] += nl + "LEI"
                    td[1] += nl + co['lei']
                    nl = "<BR>"
                if 'permid' in co:
                    td[0] += nl + "Permid"
                    td[1] += nl + "<A href='%s' target=_blank>%s</A>" % (co['permid'], re.findall('http[s]*://permid\.org/(.+)',co['permid'])[0])
                    nl = "<BR>"
                if 'open-corporates' in co:
                    td[0] += nl + "Op.Corp."
                    td[1] += nl + "<A href='%s' target=_blank>%s</A>" % (co['open-corporates'], \
                        re.findall('http[s]*://opencorporates\.com/companies/[A-Za-z_]+/(.+)',co['open-corporates'])[0])
                    nl = "<BR>"
                html += "<TD>%s</TD><TD>%s</TD>" % (td[0], td[1])  
                # end of row 
                html += "</TR>" 
                n += 1
    html += "</TABLE>"            
    return html            

def HTML_header(title = "Data Manager Development Site", extras="", params ="", width=600):
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
    <hr size=10 width=%(width)s color="#BABF57" align="left" >
    <p >&nbsp;</p>
    """
    # substitute parameters in template
    html = template % {'title': title, 'extras': extras,  'params': params, 'width': width}
    return html

def HTML_footer(debug="", environ=None):
    # standard HTML footer with back|menu link adn debugging material
    # add debugging material below a red line if debug string is non-blank
    if debug>"":
        debug = "<HR style='color: red; border-width: 1px;'><BR>" + debug
    # if no environment data, footer link simply replicates Back button     
    if environ is None:
        html = """
        <p>&nbsp;</p>
        <p style="text-decoration: underline; color: green; cursor: pointer;" onclick="window.history.back()" title="Back to previous page">Back</p>
        <P id='debug'>%(DEBUG)s</P>
        </BODY>
        </HTML>    
        """ % {'DEBUG': debug}
    else:
        # retrieve session ID and script name, then give return link to menu page
        params = cgi.parse_qs(environ['QUERY_STRING']) 
        sid = params.get('u', ['0'])[0]
        scriptnm =  environ['SCRIPT_NAME']  
        html = """
        <p>&nbsp;</p>
        <A href="https://www.gc-dz.com/%(APP)s?m=menu&u=%(SID)s" title="Return to menu">Menu</A>
        <P id='debug'>%(DEBUG)s</P>
        </BODY>
        </HTML>    
        """ % {'SID': sid, 'APP': scriptnm, 'DEBUG': debug}
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

# when run from the command line for testing, does a simple compile check
if __name__ == '__main__':
    print('-- compiled OK --')

    

    
    
    
    
    
