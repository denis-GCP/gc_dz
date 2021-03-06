"""
 ------------- gc_dz.py version 0.92 Jul 2019 ------------
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

# --- recent revision history ---
# v 0.20+ 14 May 2019 ff mods to f500_input() to change layout of fields
# v 0.25 19 Jun 2019 Link fields and download action in f500_savelink_ajax()
# v 0.26 24 Jun 2019 Form status displayed in listing, with actions done in code
# v 0.91 01 Jul 2019 Pre-production version, webform tested and working
# v 0.92 08 Jul 2019 Commodity checkbox list by companies implemented
# -------------------------------
 
# CGI interface functions for Python
# plus other common utilities
import cgi, traceback, json
import os, sys, re, datetime
import random, string
from urllib.parse import urlparse
from urllib.request import urlopen
import unidecode
import textwrap

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
        html = cmatch_tool(mdl, sid, environ)     
    elif mdl == 'f500':
        # forest 500 company lists
        html = f500_main(sid, environ)     
    elif mdl == 'f500a':
        # forest 500 past assessments
        html = f500_assess(sid, environ)     
    elif mdl == 'f500b':
        # forest 500 input/edit for 2019+
        html = f500_input(sid, environ)     
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
    qry.execute("SELECT id, menutext, module, permitflag, prtorder FROM gcdz.menus WHERE prtorder>0 ORDER BY prtorder")
    if qry.rowcount<=0:
        # if no rows, then a program/database error has occurred
        raise RuntimeError("Menu system not found!!" )
    # make template for link URL    
    t_url = '%s://%s/%s?m=%%s&u=%%s' % (environ['REQUEST_SCHEME'], environ['SERVER_NAME'],environ['SCRIPT_NAME']) 
    # create HTML page
    html = HTML_header(title="Trasepad Menu", css="f500")
    rows = qry.fetchall()     
    # create list of menu items as links, which are disabled if permit not valid
    html += "<UL>"
    for row in rows:     
        # add module name and session ID to link
        url = t_url % (row['module'], sid)
        # check if permit flag matches user
        if (row['permitflag'] & pflag) or row['permitflag']==0:
            # write line of HTML with link
            html += "<LI><A class=menu href='%s'>%s</A>" % (url, row['menutext'])
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
            ayear = postFields.get('ayear',['0'])[0] 
            cotype = postFields.get('cotype',['0'])[0]
            sid = postFields.get('sid',['0'])[0]  
            html = f500_cofind(sid, ayear, cotype, cofind)
            json_str = json.dumps({'html' : html})   
        elif actid=="f500.colink":
            # forest 500 company link tool
            mode = int(postFields.get('mode',['0'])[0])
            flid = int(postFields.get('flid',['0'])[0]) 
            ucid = int(postFields.get('ucid',['0'])[0]) 
            f500_colink_ajax(mode, flid, ucid)
            # this doesn't return anything (all actions are DB updates)
            json_str = ''   
        elif actid=="f500.comChkBox":
            # forest 500 commodity checkbox update
            sessid = postFields.get('sessid',['0'])[0]
            flid = int(postFields.get('flid',['0'])[0]) 
            cid = int(postFields.get('cid',['0'])[0]) 
            tick = int(postFields.get('tick',['0'])[0]) 
            reply = f500_comChkBox_ajax(sessid, flid, cid, tick)
            json_str = json.dumps(reply)
        elif actid == 'f500a.indNotes':
            inid   = postFields.get('inid',['0'])[0]      
            cid    = postFields.get('cid',['0'])[0]       
            sid    = postFields.get('sid',['0'])[0]       
            flid   = postFields.get('flid',['0'])[0]
            imgid  = postFields.get('imgid',['0'])[0]     
            html = f500_indNotes(inid, cid, sid, flid, imgid)
            json_str = json.dumps({'html' : html})    
        elif actid == 'f500b.update':
            # actions on the data input form.  
            sid    = postFields.get('sid',['0'])[0]       
            flid   = postFields.get('fileid',['0'])[0]
            cell   = postFields.get('cell',['0'])[0]
            text   = postFields.get('value',['0'])[0]
            reply  = f500_input_ajax(sid, flid, cell, text)
            json_str = json.dumps(reply)
        elif actid == 'f500b.select':
            sessid = postFields.get('sessid',['0'])[0]
            flid   = postFields.get('flid',['0'])[0]
            cell   = postFields.get('cell',['0'])[0]
            cell_s   = postFields.get('cell_s',['0'])[0]
            optval   = postFields.get('optval',['0'])[0]
            opttxt   = postFields.get('opttxt',['0'])[0]
            reply  = f500_select_ajax(sessid, flid, cell, cell_s, optval, opttxt)
            json_str = json.dumps(reply)
        elif actid == 'f500b.save_link':
            sessid = postFields.get('sessid',['0'])[0]
            flid   = int(postFields.get('fileid',['0'])[0])
            cell   = postFields.get('cell',[''])[0]
            inid = int(postFields.get('inid',['0'])[0])
            refid = int(postFields.get('refid',['0'])[0])
            cid = int(postFields.get('cid',['0'])[0])           
            link_text   = postFields.get('text',[''])[0]
            reply  = f500_savelink_ajax(sessid, flid, cell, inid, refid, cid, link_text)
            json_str = json.dumps(reply)
        elif actid == 'f500b.tableC':
            # gets HTML for input form.  
            inid    = postFields.get('inid',['0'])[0]       
            flid   = postFields.get('flid',['0'])[0]
            cid   = postFields.get('cid',['0'])[0]
            refid   = postFields.get('refid',['0'])[0]
            reply  = f500_input_table_ajax(inid, flid, refid, cid)
            json_str = reply  # plain HTML is expected here, no JSON wrapper
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
            userid = nextid
    else:        
        flds = qry.fetchone()
        userid = flds['userid']
    # close any open sessions for this user
    #qry.execute("UPDATE gcdz.logins SET timeout=NOW() WHERE userid=%(USERID)s AND timeout IS NULL",
    #    {'USERID': userid}) 
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
    html="""<P><SPAN style="color: green; font-weight: bold;">
    An email has been sent to %s with a link to the portal.</SPAN>  Please use this link 
    to enter the site.  It can be re-used as many times as necessary.  The link will expire
    after 30 days, and will only work from this network.  Request a new link to work from another location.</P>  
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
    
def error_page(msg, lgin=0, debug=''):
    # returns HTML for an error page, with message 'msg' in red bold
    template = '<p style="font-weight:bold; color: red">%s</p>'
    html = HTML_header("Data Manager Development Site", css="f500", width=900)
    html += (template % msg)
    if debug >'':
        html += debug
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
    smtp.set_debuglevel(1)
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
    debug=""
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
    # close any sessions more than 5 days old
    qry.execute("""UPDATE gcdz.logins SET timeout=NOW() WHERE timeout IS NULL 
        AND NOW()-timeon > '5 days' """ )
    debug += "<P>Query 1:<B>%s<P>" % qry.query    
    # if this session was started from a different IP, close it
    qry.execute("""UPDATE gcdz.logins SET timeout=NOW() WHERE sessionid=%(SID)s AND timeout IS NULL 
        AND ipaddr <> %(IP)s""" , {'SID': sid, 'IP': ip})        
    debug += "<P>Query 2:<B>%s<P>" % qry.query    
    # search for an open session    
    qry.execute("""
        SELECT U.userid, U.permit, L.timeon, L.ipaddr FROM gcdz.users AS U INNER JOIN gcdz.logins AS L
        ON U.userid=L.userid WHERE sessionid=%(SID)s AND timeout IS NULL
        """, {'SID': sid})
    debug += "<P>Query 3:<B>%s<P>" % qry.query    
    # if no match, session does not exist or has timed out
    if qry.rowcount<=0:
        debug_text = list_debug_info(debug=debug, show=False)
        html = error_page("Session ID has expired (limit 48 hours) or was from a different network - Please login again", lgin=1, debug=debug_text)
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
    html = HTML_header(title="SCTN Meta-database Tool", extras=jslib)
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
    # ---- debugging information 
    html += list_debug_info(postFields, debug, show=False)
    # create page footer
    html += HTML_footer()
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
    # ---- debugging information 
    html += list_debug_info(postFields, debug, show=False)
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
    js = '<SCRIPT src="https://www.gc-dz.com/js/f500.js"></SCRIPT>'
    # heading section of page
    html = HTML_header(css="f500", extras=js, width=1000, menulink=(sid, scriptnm), module="f500")
    # get setting of update button (True if clicked, False otherwise)
    update_btn = postFields.get('Update',[''])[0] == 'Update'
    # set default data values for controls
    dd = {'AYEAR': '0', 'COTYPE': '', 'FILTER': '', 'LISTOPT': '3'}   
    # open database cursor 
    qry = getCursor()
    if update_btn:
        # get control values from POST fields and save then as defaults
        dd['AYEAR'] = postFields.get('ayear', [''])[0]
        dd['COTYPE'] = postFields.get('cotype', [''])[0] 
        dd['FILTER'] = postFields.get('filter', [''])[0] 
        dd['LISTOPT'] = postFields.get('listopt', ['4'])[0] 
        # save as defaults 
        qry.execute("""INSERT INTO gcdz.def_data (sessionid, ddtag, ddinfo) VALUES (%(SID)s, 'F500_List', %(DD)s)   
            ON CONFLICT ON CONSTRAINT defdata_pk DO UPDATE SET ddinfo= %(DD)s 
            """, {'SID': sid, 'DD': json.dumps(dd)}) 
    else:    
        # retrieve default values if Update button was not clicked
        qry.execute("SELECT sessionid AS sid, ddtag, ddinfo FROM gcdz.def_data WHERE sessionid=%s AND ddtag='F500_List'", (sid,))
        # defaults found, retrieve them to local variables
        if qry.rowcount>0:
            flds = qry.fetchone()
            dd = flds['ddinfo']
    # HTML for form heading       
    html += """
        <FORM action="%(APP)s?m=f500&u=%(SID)s" method=POST>
        <DIV id=colist></DIV>
        """ % {'APP': scriptnm, 'SID': sid}
    # year selector
    # debug += "\ndd = %s\n" % str(dd)
    qry.execute("select distinct ayear::text, ayear from f500.dirtree union select ' All', 0 order by 1")
    html += HTML_select("Year", 10, "ayear", qry, dflt=int(dd['AYEAR']));
    # company type selector
    qry.execute("select unnest(array['Supply Chain Company', 'Financial Institution', 'All']),unnest(array['CO', 'FI', ''])")
    html += HTML_select("Company Type", 24, "cotype", qry, dflt=dd['COTYPE']);
    # Filter input selector
    html += HTML_input("Filter<SMALL> (regular expression)</SMALL>", 30, "filter", dflt=dd['FILTER'], event="onkeyup='updateColist()'");
    # list options
    qry.execute("""select unnest(array['Summary scores', 'Company Linking Tool', 'Excel filenames','2019 Assessments', 
        'Commodity checkboxes']),unnest(array['1', '2', '3', '4', '5'])""")
    html += HTML_select("Listing Format", 24, "listopt", qry, dflt=dd['LISTOPT']);
    # update button
    html += '<INPUT type="submit" name="Update" id="update" value="Update">'
    html += "<BR clear=all></FORM>"
    if update_btn:
        # create HTML table for list of companies
        # construct WHERE clause based on options
        where = ''
        # year filter must be 2019 for otion 4, otherwise as set
        if dd['LISTOPT'] != '4':
            if dd['AYEAR']>'0' : where += " ayear = %s " % dd['AYEAR']
        else:
            where += " ayear = 2019 " 
        # company type filter
        if dd['COTYPE']>'0':
            if where > '': where += " AND "
            where += " cotype = '%s' " % dd['COTYPE']
        # regular expression on company name    
        if dd['FILTER']>'':
            if where > '': where += " AND "
            where += " coname ~* '%s' " % dd['FILTER']
        # prefix with WHERE if anything set    
        if where>'':
            where = " WHERE " + where
        # query and table headings depend on List Option
        if dd['LISTOPT'] == '1':    
            # query format for commodity scores - get the company list
            query = "SELECT cotype, ayear, coname, flid FROM f500.company_file %s ORDER BY 1, 3, 2 " % where
            # get company details as a list
            qry.execute(query)
            colist = qry.fetchall()
            # get commodity and total scores for these companies
            query = "SELECT flid, cid, score, assd FROM f500.commodity_scores INNER JOIN f500.company_file USING (flid) " + where
            qry.execute(query)
            scores = qry.fetchall()
            score_lookup = makeKeylist(scores, 'flid', 'cid')
            # get commodity names and indices for headings
            qry.execute("SELECT cid, commodity FROM f500.commodities WHERE cid>0 ORDER BY cid")
            comlist = qry.fetchall()
            commodities = makeKeylist(comlist, 'cid')            
            # create table headings
            tbl = """<FORM action="%(APP)s?m=f500a&u=%(SID)s" method=POST>
                <INPUT type="hidden" name="FileID" id="fileid" value=0>
                <TABLE class=company-list><TR>
                <TH style="width: 50px">Type</TH>
                <TH style="width: 50px">Year</TH>
                <TH style="width: 250px">Company Name</TH>
                <TH style="width: 50px">File ID</TH>
                <TH style="width: 75px">Total</TH>
                """ % {'APP': scriptnm, 'SID': sid}
            # commodity headings    
            for (cid, data) in commodities.items():
                tbl +=  "<TH style='width: 75px'>%s</TH>" % data['commodity']
            # finish header row    
            tbl += "</TR>"
            # work through company list
            for co in colist:
                flid = co['flid']
                # click on row to get assessment page
                tbl += "<TR onclick='getCoAss(%s)'>" % flid
                # company type, year, name and file ID
                tbl += "<TD>%s</TD>" % co['cotype']
                tbl += "<TD>%s</TD>" % co['ayear']
                tbl += "<TD>%s</TD>" % co['coname']
                tbl += "<TD>%s</TD>" % flid
                # total score - applicable to all types
                key = '%s+0' % flid
                # check key value OK
                if key in score_lookup:
                    tbl += HTML_score_td(score_lookup[key]['score'])
                    # if type CO, give commodity scores
                    if co['cotype']=='CO':
                        for cid in commodities.keys():
                            # get score for this company and commodity
                            key = '%s+%s' % (flid, cid)
                            assd = score_lookup[key]['assd']
                            # check if it was assessed (YES or 1)
                            if re.search('yes|1', str(assd), re.I):
                                tbl += HTML_score_td(score_lookup[key]['score'])
                            else:
                                tbl += "<TD class=center>-</TD>"             # not assessed - hyphen/dash
                    else:
                        # financial institution - no commodity scores - display rest or row as blank       
                        tbl += "<TD colspan=%s>&nbsp;</TD>" % len(comlist)
                else:
                    # missing key value - output diagnostic message
                    tbl += "<TD colspan=%s style='text-align:center'>--- Assessment not available ---</TD>" % (len(comlist)+1)                            
                # end of row               
                tbl += "</TR>"
        elif dd['LISTOPT'] == '2': 
            # query format for grouping tool  ----- to be amended --------
            query = """SELECT a.cotype, a.ayear, a.coname, c.main, a.flid, c.ucid  FROM f500.company_file AS a 
                LEFT JOIN f500.coflids AS c USING (flid) %s ORDER BY 1, 3, 2  """ % where
            # get company details
            qry.execute(query)
            # create table headings
            tbl = """<FORM action="%(APP)s?m=f500a&u=%(SID)s" method=POST>
                <INPUT type="hidden" name="FileID" id="fileid" value=0>
                <TABLE class=company-list style="width: 800px"><TR>
                <TH style="width: 50px">Type</TH>
                <TH style="width: 50px">Year</TH>
                <TH style="width: 350px">Company Name</TH>
                <TH style="width: 50px">Main</TH>
                <TH style="width: 75px">File ID</TH>
                <TH style="width: 75px">Co ID</TH>
                <TH style="width: 75px">Use</TH>
                <TH style="width: 75px">Link</TH>
                <TH style="width: 75px">Unlink</TH>
                </TR>""" % {'APP': scriptnm, 'SID': sid}
            # create table body
            for row in qry:
                # unique company id set to empty string if undefined
                ucid = str(row['ucid']) if row['ucid'] is not None else ''
                flid = str(row['flid'])
                # start of a table row
                tbl += "<TR>"
                # company type, year, company name
                tbl += "<TD title='supply chain company(CO) or financial institution/investor(FI)'>%s</TD>" % row['cotype']
                tbl += "<TD>%s</TD>" % row['ayear']
                tbl += "<TD>%s</TD>" % row['coname']
                # checkbox for 'main' name, ticked if main is True, calls clickMain in f500.js when changed
                chk = 'checked' if row['main'] else ''
                tbl += """<TD title='tick if preferred name' style="text-align:center">
                    <INPUT type=checkbox id=main%s onchange='clickMain(this)' %s></TD>"""  % (flid, chk)
                # file and company ID
                tbl += "<TD title='company file ID for year' style='text-align:right'>%s</TD>" % flid
                # Unique company ID
                tbl += "<TD id=ucid%s title='unique company ID' style='text-align:right'>%s</TD>" % (flid, ucid)
                # use this Co ID for next link
                tbl += """<TD title='tick if company to link with' style="text-align:center">"""
                # 'target' checkbox, only shown if ucid is set    
                if ucid > '':
                    tbl += "<INPUT type=checkbox class=ucid-link id=ucid-link-%s value=%s onchange='clickUcidLink(this)'>"  % (flid, ucid) 
                tbl += "</TD>"
                # action buttons to link or unlink companies
                tbl += """<TD style='text-align:center'><INPUT type=button class=tiny onclick='linkButton(%s)' 
                    title='click to link to main co.'></TD>""" % flid
                tbl += """<TD style='text-align:center'><INPUT type=button class=tiny onclick='unlinkButton(%s)' 
                    title='click to unlink from main co.'></TD>""" % flid                 
                tbl += "</TR>"
        elif dd['LISTOPT'] == '3': 
            # query format with file names
            query = """SELECT d.cotype, d.ayear, x.coname, x.flid, f.flname, DATE(f.fltime) FROM f500.xlcohdr AS x 
                INNER JOIN f500.filelist AS f ON x.flid=f.flid INNER JOIN f500.dirtree AS d ON f.dtid=d.dtid
                %s ORDER BY d.cotype, x.coname, d.ayear """ % where
            # get company details
            qry.execute(query)
            # create table headings
            tbl = """<FORM action="%(APP)s?m=f500a&u=%(SID)s" method=POST>
                <INPUT type="hidden" name="FileID" id="fileid" value=0>
                <TABLE class=company-list><TR>
                <TH style="width: 50px">Type</TH>
                <TH style="width: 50px">Year</TH>
                <TH style="width: 250px">Company Name</TH>
                <TH style="width: 50px">File ID</TH>
                <TH style="width: 350px">Filename</TH>
                <TH style="width: 100px">Last Update</TH>
                </TR>""" % {'APP': scriptnm, 'SID': sid}        
            # create table body
            for row in qry:
                tbl += "<TR onclick='getCoAss(%s)'>" % row['flid']
                for col in row:
                    tbl += "<TD>%s</TD>" % col
                tbl += "</TR>"
        elif dd['LISTOPT'] == '4': 
            # 2019 assessments
            query = """SELECT d.cotype, d.ayear, x.coname, x.flid, s.status, DATE(s.lastupd), u.email FROM f500.xlcohdr AS x 
                INNER JOIN f500.filelist USING (flid) 
                INNER JOIN f500.dirtree AS d USING (dtid)
                LEFT JOIN (f500.survey_status INNER JOIN f500.survey_status_texts USING (statid)) AS s USING (flid)
                LEFT JOIN gcdz.logins USING (sessionid) 
                LEFT JOIN gcdz.users AS u USING (userid) 
                %s ORDER BY d.cotype, x.coname """ % where
            # get company details
            qry.execute(query)
            # create table headings
            tbl = """<FORM action="%(APP)s?m=f500b&u=%(SID)s" method=POST>
                <INPUT type="hidden" name="FileID" id="fileid" value=0>
                <TABLE class=company-list><TR>
                <TH style="width: 50px">Type</TH>
                <TH style="width: 50px">Year</TH>
                <TH style="width: 250px">Company Name</TH>
                <TH style="width: 50px">File ID</TH>
                <TH style="width: 150px">Status</TH>
                <TH style="width: 100px">Last Updated</TH>
                <TH style="width: 150px">By</TH>
                </TR>""" % {'APP': scriptnm, 'SID': sid}        
            # create table body
            for row in qry:
                tbl += "<TR onclick='getCoAss(%s)'>" % row['flid']
                for col in row:
                    tbl += "<TD>%s</TD>" % col
                tbl += "</TR>"
        elif dd['LISTOPT'] == '5': 
            # commodity checkboxes
            qry.execute("select cid, commodity from f500.commodities where cid>0 order by cid")
            cdlist = qry.fetchall()
            query = """SELECT cotype, ayear, coname, flid, array_agg(cid ORDER BY cid) as commodities FROM f500.company_file  
                LEFT JOIN f500.comtraders USING (flid) %s GROUP BY 1,2,3,4 ORDER BY 1, 3, 2""" % where
            # get company details
            qry.execute(query)
            rows = qry.fetchall()
            # create table headings
            tbl = """<FORM action="%(APP)s?m=f500b&u=%(SID)s" method=POST>
                <INPUT type=hidden id=sessionid name=sessionid value=%(SID)s>
                <TABLE class=company-list><TR>
                <TH style="width: 50px">Type</TH>
                <TH style="width: 50px">Year</TH>
                <TH style="width: 250px">Company Name</TH>
                <TH style="width: 50px">File ID</TH>
                """ % {'APP': scriptnm, 'SID': sid}
            for cd in cdlist:
                tbl += "<TH class=commodities>%s</TH>" % cd['commodity']
            tbl += "</TR>"    
            # create table body
            for row in rows:
                tbl += "<TR>"
                tbl += "<TD>%s</TD>" % row['cotype']
                tbl += "<TD>%s</TD>" % row['ayear']
                tbl += "<TD>%s</TD>" % row['coname']
                tbl += "<TD>%s</TD>" % row['flid']
                cd = row['commodities']
                for c in range(len(cdlist)):
                    if c+1 in row['commodities']:
                        chk = 'checked'
                    else:
                        chk = ''
                    tbl += "<TD class=commodities><INPUT type=checkbox id=chk-%s-%s %s onchange='updateCommodity(this)'></TD>" \
                        % (row['flid'], c+1, chk)            
                tbl += "</TR>"
        else:
            raise RuntimeError("Unrecognised option code '%s'!" % dd['LISTOPT'])        
        tbl += "</TABLE></FORM>"          
    else:    
        # no list to display yet
        tbl = "<P><SMALL>Make a selection from the drop down lists and click <B>Update</B> to see the company list...</SMALL></P>"
    html += tbl
    # ---- debugging information 
    html += list_debug_info(postFields, debug, show=True)
    html += HTML_footer(environ)
    return html

def makeKeylist(alist, akey, akey2=''):
    # given a list of dictionaries, converts it to a dictionary of dictionaries,
    # using 'aKey' as the lookup key.  if second key is given, they are joined with +
    klist={}
    for row in alist:
        if akey2=='':
            # single key
            klist[row[akey]] = row
        else:
            # two keys, key+key2
            klist['%s+%s' % (row[akey], row[akey2])] = row
    return klist    

def HTML_score_td(score, css_class='center'):
    # safe conversion of a score to a centered table cell value.  Values that are not
    # valid floating points are returned as strings, in a TD wrapper
    try:
        f = float(score)
        if f>0.0 and f<1.0:
            # output as a percentage if between 0 and 1
            p = 100.0 * f
            td = "<TD class=%s>%s%%</TD>" % (css_class, "{:5.1f}".format(p))             
        else:
            # otherwise output as a raw score to 3 sig figs.    
            td = "<TD class=%s>%s</TD>" % (css_class, "{:5.3g}".format(f))
    except (ValueError) as msg:
        # not a number - output as is
        td = "<TD class=%s>%s</TD>" % (css_class, score)
    return td
        
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
    
def f500_cofind(sid, ayear, cotype, cofind):
    # pre-view of companies selected by a regex, also limited by year and company type
    # This is an Ajax routine, called via ajaxHandler() and javascript updateColist() in f500.js.
    # It services the regex preview function for company lists
    # construct WHERE clause based on options
    where = ''
    if ayear>'0' : where += " d.ayear = %s " % ayear
    if cotype > '0':
        if where > '': where += " AND "
        where += " d.cotype = '%s' " % cotype
    if cofind > '':
        if where > '': where += " AND "
        where += " x.coname ~* '%s' " % cofind
    if where>'':
        where = " WHERE " + where
    # main query text
    query = """SELECT DISTINCT x.coname FROM f500.xlcohdr AS x 
        INNER JOIN f500.filelist AS f ON x.flid=f.flid 
        INNER JOIN f500.dirtree AS d ON f.dtid=d.dtid
        %s ORDER BY 1 LIMIT 20 """ % where
    qry = getCursor()
    qry.execute(query)
    if qry.rowcount == 0:
        html = "<P>[No matches found]</P>"
        #html += "<P>%s</P>" % query     # diagnostic only
    else:
        flds = qry.fetchall()
        # list companies found  
        html = "<UL>"  
        for co in flds:
            html += "<LI>%s" % co[0]
        html += "</UL>"  
        if qry.rowcount>=20:
            html += "<P>...(more)...<P>"
    return html        

def f500_colink_ajax(mode, flid, ucid=0):
    # Company Linking Tool on F500 Listing, called from f500_main()
    # mode can be (1) set 'main' flag for flid in coflids table
    # (2) link current flid with ucid
    # (3) unset ucid for flid
    # (4) clear main flag for flid
    qry = getCursor()
    if mode==1:
        qry.execute("UPDATE f500.coflids SET main=TRUE WHERE flid=%s", (flid,))
    elif mode==2:
        qry.execute("UPDATE f500.coflids SET ucid=%s WHERE flid=%s", (ucid, flid))
    elif mode==3:
        qry.execute("UPDATE f500.coflids SET ucid=NULL WHERE flid=%s", (flid,))
    elif mode==4:
        qry.execute("UPDATE f500.coflids SET main=FALSE WHERE flid=%s", (flid,))
    # nothing to return - any errors handled in ajaxHandler()
    #raise RuntimeError(qry.query)
    qry.connection.commit()

def f500_comChkBox_ajax(sessid, flid, cid, tick):
    reply = f500_edit_check(sessid)
    if reply['valid'] == 1:
        qry = getCursor()
        try:
            # if tick is 1, add commodity to table comtraders, if 0 delete it
            if tick==1:
                qry.execute("INSERT INTO f500.comtraders (flid, cid) VALUES(%s, %s) ON CONFLICT DO NOTHING", (flid, cid))
            elif tick==0:    
                qry.execute("DELETE FROM f500.comtraders WHERE flid=%s AND cid=%s", (flid, cid))
            else:
                raise RuntimeError("Invalid tick value (%s)" % tick) 
            qry.connection.commit()      
        except Exception as e:
            # give traceback and diagnostics
            reply['msg'] = str(e)
            reply['debug'] = "<PRE>\n" + traceback.format_exc() + "\n</PRE>"
            reply['valid'] = 0
    return reply  
      
def cmatch_tool(module, sid, environ):
    # implements trase Neural alpha name matching tools
    # the code is borrowed and adapted from SCTN tool, it shares similar style and mode of action
    # get POST data if any and the name of the calling app (dev or exec)
    postFields = getPostFields(environ)
    # set default POST values if none given
    if len(postFields)==0:    
        # set postFields to some default columns
        postFields = {'Mode': ['None']}  
    # get a cursor for database queries     
    qry = getCursor()
    # HTML for debugging info - left blank if none
    debug = ""
    # name of calling script (exec or dev)
    scriptnm =  environ['SCRIPT_NAME']  
    js = '<SCRIPT src="https://www.gc-dz.com/js/cmatch.js"></SCRIPT>'
    # heading section of page
    html = HTML_header(css="f500", extras=js, width=1000, menulink=(sid, scriptnm), module=module)
    # segment of html for text box to paste company list
    # set default data values for controls
    dd = {'CLIST': '', 'TRASE_CHK': 'checked', 'INFO_CHK': ''}    
    # check if the 'GO' button has been clicked, and if so replace defaults with passed data
    if postFields.get('Search',[''])[0] == 'Go':    
        # save current settings as default values
        dd['CLIST'] = postFields.get('clist',[''])[0]
        dd['TRASE_CHK'] = 'checked' if postFields['search_opt'][0] == 'trase' else ''
        dd['INFO_CHK'] = 'checked' if postFields['search_opt'][0] == 'info' else ''
        qry.execute("""INSERT INTO gcdz.def_data (sessionid, ddtag, ddinfo) VALUES (%(SID)s, 'cmatch', %(DD)s)   
            ON CONFLICT ON CONSTRAINT defdata_pk DO UPDATE SET ddinfo= %(DD)s 
            """, {'SID': sid, 'DD': json.dumps(dd)}) 
        # get list of names to check, method, and output format
        nmlist =  re.split(r'\r\n', dd['CLIST'])
        chktype =  postFields['search_opt'][0]
        # results of name check are returned as HTML
        html_out = nameCheck(nmlist, chktype)
    else: 
        # no output yet
        html_out = ""            
        # retrieve default values if GO was not clicked (as on first entry to this tool)
        qry.execute("SELECT sessionid AS sid, ddtag, ddinfo FROM gcdz.def_data WHERE sessionid=%s AND ddtag='cmatch'", (sid,))
        # defaults found, retrieve them to local variables
        if qry.rowcount>0:
            flds = qry.fetchone()
            dd = flds['ddinfo']
    # HTML for displayed form        
    html += """
    <FORM action="%(SCRIPT)s?m=cmatch&u=%(SID)s" method=POST>
    <BR>&nbsp;<BR>
    <TABLE class=match><TR style="vertical-align: top">
        <TD>
            <TEXTAREA rows=14, cols=70 name="clist">%(CLIST)s</TEXTAREA>
        </TD><TD style="font-size: small; padding-left: 30px;">
            Type or paste a list of names. For quick lookup, type a single name and click the GO button.
            <BR>&nbsp;<BR>
            <U>Method</U><BR>
            <INPUT type="radio" value="trase" name="search_opt" %(TRASE_CHK)s>&nbsp;&nbsp;Match to names in trase<BR>
            <INPUT type="radio" value="info" name="search_opt" %(INFO_CHK)s>&nbsp;&nbsp;Get company info and links
            <BR>&nbsp;<BR>
            <INPUT type="submit" name="Search" value="Go" onclick="$('#wait_msg').css('display','block')">
        </TD><TD>
    </TR></TABLE>
    <P id=wait_msg>Please wait : Fetching data...</P>
    </FORM>        
    """ % {'SCRIPT': scriptnm, 'SID': sid, 'CLIST': dd['CLIST'], 'TRASE_CHK': dd['TRASE_CHK'], 'INFO_CHK': dd['INFO_CHK']}
    # add output results to page
    html += html_out
    # ---- debugging information 
    html += list_debug_info(postFields, debug, show=False)
    html += HTML_footer(environ)
    return html

def f500_input(sid, environ):
    # handles direct input of Forest 500 assessment to database
    # --- note: this originally copied and modifed from f500_assess() procedure ---
    # show only top level here as a simple DIV.  All the detail is done via ajax (f500_ajax)
    postFields = getPostFields(environ)
    # name of calling script (exec or dev)
    scriptnm =  environ['SCRIPT_NAME']  
    # HTML for debugging info - left blank if none
    debug = ""
    fileid = int(postFields.get('FileID',[0])[0])
    # include f500.js library  in HTML header
    js = '<SCRIPT src="https://www.gc-dz.com/js/f500.js"></SCRIPT>'
    # get company name
    qry = getCursor()
    qry.execute("""SELECT d.cotype, d.ayear, x.coname, x.flid, f.flname, DATE(f.fltime) FROM f500.xlcohdr AS x 
                INNER JOIN f500.filelist AS f ON x.flid=f.flid INNER JOIN f500.dirtree AS d ON f.dtid=d.dtid
                WHERE f.flid=%s """, (fileid,))
    if qry.rowcount==0:
        # show error heading, message and Close button
        html =  HTML_header(title= "Forest 500 Data Input - Error", width=1000, css="f500", extras=js, menulink = (sid, scriptnm))
        html += "<P class=error style='display: block'>Company ID not specified</P>" 
        html += "<p>&nbsp;</p><INPUT type=button value='Close' onclick='window.close()'>"
        html += "</BODY></HTML>"    
        return html
    # get company details and construct page heading    
    co = qry.fetchone()
    title = "Forest 500 Data Input %s : %s" % (co['ayear'], co['coname'])
    html =  HTML_header(title, width=1000, css="f500", extras=js, menulink = (sid, scriptnm))
    # current year as integer
    cyear = int(co['ayear'])
    # cells used for company headers, years 2014-2019 (0-5), company type CO, FI (0-1) 
    cell_cn = [['C8','C8'],['C8','C8'],['C9','C8'],['C9','B3'],['C4','C3'],['C4','C3']]             
    cotypes = ["CO", "FI"]
    t = cotypes.index(co['cotype']) 
    y = cyear - 2014
    # standard javascript events to go on all INPUT fields, needs cell address for %s
    jslink = "onkeydown='input_key(event, this)' onchange='input_save(this, \"%s\")'" 
    # these hidden fields are for the f500.js routine input_save() to pick up
    html += "<INPUT type=hidden id=fileid name=fileid value=%s>" % fileid
    html += "<INPUT type=hidden id=sessionid name=sessionid value=%s>" % sid
    html += "<INPUT type=hidden id=scriptnm name=scriptnm value=%s>" % scriptnm
    # if company has not been edited before, update 'viewed' status
    qry.execute("UPDATE f500.survey_status SET statid=1, sessionid=%s, lastupd=now() WHERE flid=%s AND statid in (0, 1)", \
        (sid, fileid))
    # advisory text at top of page (below header section)
    html += """<SMALL>All edits are tracked and saved as they are made.  </SMALL>"""  
    html += "<BR clear=all>"
    # company name display/edit control
    html += HTML_input("Company Name", 50, "coname", co['coname'], (jslink % cell_cn[y][t]))
    # view used to display indicator info 
    # get range of indicator IDs from year and company type indices 
    # (eg 2019 CO, y=5, t=0, (5+1)*10000+(0+1)*1000 = 61000)
    ind_from = (y+1)*10000 + (t+1)*1000
    ind_to =ind_from + 999
    # display file ID   
    html += """<P style="float: left; padding-left: 300px; padding-right: 20px; font-size: xx-small; 
        text-align: right">File ID<BR>%s</P>""" % fileid
    # action buttons - Close  
    html += """&nbsp;<INPUT type=button value='Close' onclick='window.close()' 
            style='margin-left: 300 px; margin-top: 10px'>"""      
    html += "<BR clear=all>"
    # first indicator group, not commodity specicific
    qry.execute("""SELECT igid, heading FROM f500.ind_groups WHERE ayear=%s AND cotype=%s AND indgrp=1
        """, (co['ayear'], co['cotype']))
    html += "<TABLE class=ind-tbl>"
    # this blank row controls fixed column widths (set in CSS, see TR.ind-tbl-H)
    # bar symbols can be changed to spaces onnce everything is OK
    html += """<THEAD><TR class=ind-tbl-H>
        <TH>&nbsp;</TH>
        <TH>&nbsp;</TH>
        <TH>&nbsp;</TH>
        <TH>&nbsp;</TH>
        <TH>&nbsp;</TH>
        </TR></THEAD>""" 
    html += "<TBODY>" 
    # group 1 indicators with heading button
    gp = qry.fetchone()
    name= "IG%s" % gp['igid']
    js = 'toggleIndGroup(%s)' % gp['igid']
    btn_text = HTML_clean(gp['heading'])
    btn_val = HTML_clean(gp['igid'])
    html += """<TR class=btn-row><TD colspan=5>%s</TD></TR>""" % HTML_button(name, btn_text, btn_val,'ind-gp-btn', js)     
    # retrieve indicator details for this group
    qry.execute("""SELECT DISTINCT inid, indgrp, indnum, indtext, guide, scoring, maxpts, atype FROM f500.ind_main 
              INNER JOIN f500.ind_detail USING (inid) WHERE inid/100 = %s ORDER BY 1""",(int(gp['igid']),))
    indlist = qry.fetchall()
    for ind in indlist:
        # generate HTML for main indicator description
        (htm, dbg) =  f500_input_indrow(ind, fileid)
        html += htm
        debug += dbg
    # commodity specific indicators group button
    html += "<TR class=btn-row>"    
    html += "<TD colspan=5>"
    html += HTML_button("ComIndBtn", "Commodity-specific Indicators", 1,'ind-gp-btn') 
    html += "</TD></TR>"     
    # commodity specific indicators list
    qry.execute("""SELECT d.cid, coalesce(d.sid, m.inid, 9999999) AS refid, m.inid,  c.commodity, m.indgrp, 
        m.indnum, m.indtext, m.guide, m.scoring, d.detid, d.atype
    	FROM f500.ind_main AS m INNER JOIN f500.ind_detail AS d USING (inid) 
    	INNER JOIN f500.commodities AS c USING (cid)
    	INNER JOIN f500.comtraders as t ON t.flid=%s AND t.cid=c.cid
    	WHERE d.cid IS NOT NULL AND cotype=%s AND ayear=%s
    	ORDER BY 1,2,3;""",(fileid, co['cotype'], cyear))
    indlist = qry.fetchall()
    # initialise variables to detect change in commodity, cross-referenced indicators
    last_cid=0   # do new commodity heading when this changes
    last_inid=0  # do not indent indicator text when this changes
    for ind in indlist:
        # test for new commodity group
        if ind['cid']!=last_cid:
            # do commodity heading
            html += """<TR id=comhdr-%(ID)s ><TD colspan=2></TD>
                <TD  class=comhdr onclick="toggleCommodity(%(ID)s)">%(LABEL)s</TD><TD colspan=2></TD>
                </TR>""" % {'ID': ind['cid'], 'LABEL': ind['commodity']}
            last_cid = ind['cid']
        # set indent status - True if same main indicator, False if a new main indicator
        if ind['refid']==last_inid:
            indent = True
        else:
            last_inid = ind['refid']
            indent = False    
        # generate HTML for main indicator description
        (htm, dbg) =  f500_input_indrow(ind, fileid, indent, ind['cid'])
        html += htm
        debug += dbg
    # --- a bit more code needed below here for remining non-commodity indicators ---    
    # headings for remaining non-commodity specific indicators
    sql = """SELECT DISTINCT igid, heading FROM f500.ind_detail 
            INNER JOIN f500.ind_groups AS g ON (inid/100)::int4=igid
            WHERE cid IS NULL AND  g.indgrp>1  AND g.ayear=%s AND g.cotype=%s
            ORDER BY 1"""
    qry.execute(sql, (co['ayear'], co['cotype']))
    groups = qry.fetchall()
    # loop though remaining non-commodity groups
    for gp in groups:
        # do group heading button
        name= "IG%s" % gp['igid']
        js = 'toggleIndGroup(%s)' % gp['igid']
        btn_text = HTML_clean(gp['heading'])
        btn_val = HTML_clean(gp['igid'])
        html += """<TR class=btn-row><TD colspan=5>%s</TD></TR>""" % HTML_button(name, btn_text, btn_val,'ind-gp-btn', js)     
        # retrieve indicator details for this group
        qry.execute("""SELECT DISTINCT inid, indgrp, indnum, indtext, guide, scoring, maxpts, atype FROM f500.ind_main 
            INNER JOIN f500.ind_detail USING (inid)
            WHERE inid/100 = %s ORDER BY 1""",(int(gp['igid']),))
        indlist = qry.fetchall()
        for ind in indlist:
            # generate HTML for main indicator description
            (htm, dbg) =  f500_input_indrow(ind, fileid)
            html += htm
            debug += dbg	        
    # finish off table layout
    html += "</TBODY></TABLE>"             
    #html += "</FORM>"
    # show window close button at bottom of page
    html += "<p>&nbsp;</p><INPUT type=button value='Close' onclick='window.close()'>"
    # pop-up notes DIV.  This positioned and populated in Javascript/ajax (see f500_indNotes).
    html += "<DIV id=ind-notes></DIV>"	
    # ---- debugging information 
    see_dbg = scriptnm=='/dev'  # shows for /dev, hidden for /exec
    html += list_debug_info(postFields, debug, show=see_dbg)
    html += "</BODY></HTML>"    
    return html
    
def f500_input_indrow(ind, fileid, indent=False, cid=0):
    # Handles the indicator row setup for the F500 data input page.
    # Called from 'f500_input()' for each indicator.
    # Based on and adapted from 'HTML_indMain()' to handle specifics of data  input.
    # 'ind' has fields inid, indgrp, indnum, indtext, guide, scoring, maxpts
    # fileid is file being processed.  cdlist is a list of applicable commodity indices, or None 
    # if commodities not applicable to this file, ie a financial institution
    #
    debug=""
    inid = ind['inid']      # shorthand as field for Indicator ID is referenced repeatedly
    # first get rid of 'dangerous' characters in the text fields, but keep line breaks.
    text = HTML_clean(ind['indtext'], br=True)
    guide = HTML_clean(ind['guide'], br=True)
    scoring = HTML_clean(ind['scoring'], br=True)
    if cid>0 :
        if indent:
            # add cross-refenced indicator to row ID to make an ID that will be unique on the page
            xref = str(ind['refid'])[2:]
            rowid = "%s-%s-%s" %  (inid, xref, cid)            
        else:
            # make row ID from indicator ID and commodity ID
            rowid = "%s-%s" % (inid, cid)
    else:    
        rowid = inid
    # now create the table structure with the cleaned up text for the 'indicator' part of the first row
    if indent:
        html = """ <TR class=ind-row-A id=ind-A-%(INID)s >
            <TD style="background-color: white">&nbsp;</TD>
            <TD style="background-color: hsl(177, 50%%, 80%%)">%(GP)s.%(NUM)s</TD>
            <TD style="background-color: hsl(177, 50%%, 80%%)" colspan=2>%(TEXT)s</TD> 
            <TD style="text-align: right; background-color: hsl(177, 50%%, 80%%)">
            <IMG src='img/note_btn.png' class=icon-tiny onclick='toggleIndTable2("%(INID)s","B")'>
            <IMG src='img/edit_btn.png' class=icon-tiny onclick='toggleIndTable2("%(INID)s","C"); getTableC("%(INID)s")'>
            </TD></TR>
            """ % {'INID': rowid,  'GP': ind['indgrp'], 'NUM': ind['indnum'], 'TEXT': text}    
    else:
        html = """ <TR class=ind-row-A id=ind-A-%(INID)s >
            <TD >%(GP)s.%(NUM)s</TD>
            <TD  colspan=3>%(TEXT)s</TD>
            <TD style="text-align: right">
            <IMG src='img/note_btn.png' class=icon-tiny onclick='toggleIndTable2("%(INID)s","B")'>
            <IMG src='img/edit_btn.png' class=icon-tiny onclick='toggleIndTable2("%(INID)s","C"); getTableC("%(INID)s")'>
            </TD></TR> 
            """ % {'INID': rowid,  'GP': ind['indgrp'], 'NUM': ind['indnum'], 'TEXT': text}
    # populate second row with additional text        
    html += """
        <TR class=ind-row-B id=ind-B-%(INID)s>            
            <TD>&nbsp;</TD>            
            <TD colspan=3>%(GUIDE)s</TD>
            <TD colspan=1>%(SCOR)s</TD>
        </TR>
    """ % {'INID': rowid, 'GUIDE': guide, 'SCOR': scoring}
    # third row receives sub-table for indicator data entry.  This is created via f500.js ajax: see getTableC()
    refid = ind.get('refid', 0)
    #tbl = f500_input_indata(inid, fileid, refid, cid)
    html += """
        <TR class=ind-row-C id=ind-C-%(INID)s>
            <TD class=super-feint>
                <INPUT id=C-cid-%(INID)s  type=hidden value=%(CID)s>
                <INPUT id=C-flid-%(INID)s type=hidden value="%(FLID)s">
                <INPUT id=C-refid-%(INID)s  type=hidden value=%(REFID)s>
                %(ATYPE)s
            </TD>            
            <TD id=Tbl-C-%(INID)s colspan=4>&nbsp;</TD>
        </TR>
    """ % {'INID': rowid, 'FLID': fileid, 'REFID': refid, 'CID': cid, 'ATYPE': ind['atype']}
    return (html, debug)    

def f500_input_table_ajax(inid, flid, refid=0, cid=0):
    # sub-part of f500_input page, comprising a table for one indicator (inid) and company (flid)
    # which may be cross-referencng a main indicator (refid) and commodity specific (cid)
    # Returns an HTML layout with javascript linkage to handle I/O via f500_input_ajax
    # for indicator assessment data entry. 
    # called as an ajax routine via f500.js getTableC() and python ajaxHandler()  
    # if debug string contains anything, returns that instead
    debug = ""
    try:
        # get SQL cursor for query
        qry = getCursor()
        # make sure numbers are numbers
        refid=int(refid)
        cid=int(cid)
        # and indicator id does not include commodity postfix
        if inid.find('-')>=0:
            inid = inid.split('-')[0]
        # commodity modifier for WHERE clause if cid is given
        wh_cid = " AND cid=%s " % cid if cid>0 else ""
        # sub-indicator modifer if refid is not the same as inid
        wh_sid = " AND sid = %s " % refid if refid>0 and str(refid)!=inid else ""
        sql_a = """SELECT atype, ptid, ptlabel, ptype, alist, layout, concat(xlcol, xlrow::TEXT) AS xlcell 
	        FROM f500.assmnt_parts INNER JOIN f500.ind_detail USING (atype) 
            WHERE inid=%(INID)s %(CID)s %(SID)s
            """ % {'INID': inid, 'CID': wh_cid, 'SID': wh_sid}
        sql = """SELECT atype, ptid, ptlabel, ptype, alist, layout, a.xlcell, xltext 
            FROM (%s) AS a LEFT JOIN f500.xldata AS x ON a.xlcell=x.xlcell AND x.flid=%s ORDER BY ptid
        """  % (sql_a, flid)   
        qry.execute(sql)
        rows = qry.fetchall()
        # save query text for debug output if an exception occurs
        debug += "<P>%s</P>" %  sql    
        if qry.rowcount==0:
            raise RuntimeError("Input spec. not defined for this indicator!" )
        # start HTML for input form table
        tbl = "<TABLE class=ind-sub-tbl>"
        (rnum, cnum, rspan) = (0, 0, 0)       # row and column counters, end column
        # work through specification rows
        for row in rows:
            # start a new row when column counter is zero
            if cnum==0:
                tbl += "<TR>"
                rnum -= 1
                if rnum<1 : rnum = 1  
            # cell address for this data
            cell = row['xlcell']
            # get layout info to construct TD tag
            lyt = row['layout']
            if lyt is None:
                raise RuntimeError("Bad input spec. for this indicator! [atype=%s]" % row['atype'])            
            # get rowspan indications defined after * separator (must be at end of string)
            k = lyt.find('*')
            if k >= 0:
                rspan =int(lyt[k+1:])
                rnum = rspan
            else: 
                rspan = 1 
            # get colspan based on + separator 
            cspan = lyt.count('+') + 1
            # TD spacification
            tbl += "<TD colspan=%s rowspan=%s>" % (cspan, rspan)
            cnum += cspan
            # label for this cell
            tbl += "<LABEL class=ptlabel>%s</LABEL><BR>" % row['ptlabel']
            # replace 'None' with blank where no data
            xltext = '' if row['xltext'] is None else row['xltext']    
            # content depends on ptype letter
            if row['ptype']=='A':
                # drop down box - look for match B (single select) or C (multiselect) scores
                for r in range(len(rows)):
                    if rows[r]['ptype']=="B":
                        # B signifies single select option list
                        #multi = ""
                        multi=0
                    elif rows[r]['ptype']=="C":
                        # C signifies multi select option list so add keyword for HTML SELECT tag
                        # multi = "multiple"
                        multi=1
                    else:
                        # not a relevant ptype so continue to evaluate next one
                        continue    
                    # set array of scores and the cell reference for scores, then break out of loop    
                    scores = rows[r]['alist']
                    cells = rows[r]['xlcell']
                    break
                else:
                    # a B or C record is required if there is a type A, so flag error if not found
                    raise RuntimeError("Input spec. error: Missing list of scores (ptid=%s)!" % row['ptid'])
                onchg = 'indata_opt(this, %s, "%s")'  % (flid, cells)    
                tbl += HTML_checklist(cell, row['alist'], xltext, onchg, scores, multi=multi)    
            if row['ptype']=='B' or row['ptype']=='C':
                # these are both scores from either single or multi-select drop downs, and have the same action
                tbl += "<P class=score id=%s>%s</P></TD>" % (row['xlcell'], xltext)
            if row['ptype']=='D' or row['ptype']=='E':
                # single (D) or multi-(E) select  lists - same except for MULTIPLE keyword on tag
                multi = row['ptype']=='E'
                onchg = 'indata_list(this, %s)'  % flid    
                tbl += HTML_checklist(cell, row['alist'], xltext, onchg, multi=multi)    
            if row['ptype']=='F':            
                #free text input
                text_div = """<DIV id=indata-%(CELL)s class=indata contenteditable=true onkeydown='input_key(event, this)'   
                   onblur='input_save(this, cell="%(CELL)s")'> %(TEXT)s</DIV>""" 
                tbl += text_div % {'TEXT': xltext, 'CELL': row['xlcell']}
            if row['ptype']=='G':            
                # link validation and formatting 
                text_div = """<DIV id=link-%(CELL)s class=indata contenteditable=true onkeydown='input_key(event, this)'   
                   onblur='input_save_link(this, %(INID)s, %(SID)s, %(CID)s)'>%(TEXT)s</DIV>""" 
                tbl += text_div % {'TEXT': xltext, 'CELL': row['xlcell'], 'INID': inid, 'CID': cid, 'SID': refid}
            if row['ptype']=='H':
                # date input and validation (allows eg 23 jun 2019, 23/6/2019, Jun 2019, 6/2019)
                text_div = """<INPUT type=text id=indata-%(CELL)s class=date value="%(TEXT)s"
                   onchange='input_save_date(this, cell="%(CELL)s")'> """ 
                tbl += text_div % {'TEXT': xltext, 'CELL': row['xlcell']}                    
            if row['ptype']=='I':
                # number + units input and validation (allows eg 123,456 mt, 2003.456 kg )
                text_div = """<INPUT type=text id=indata-%(CELL)s class=num value="%(TEXT)s"
                   onchange='input_save_num(this, cell="%(CELL)s")'> """ 
                tbl += text_div % {'TEXT': xltext, 'CELL': row['xlcell']}                    
            tbl += "</TD>"            
            # check for end of row 
            if rnum==1 and cnum>=3:
                # no row spanning, reset column counter and put in TR end tag
                cnum = 0
                tbl += "</TR>"
            elif rnum>1 and cnum>=2:
                # row spanning in column 3, so also end of row condition  after column 2
                cnum = 0
                tbl += "</TR>"
        # finish HTML table
        tbl += "</TABLE>"
    except RuntimeError as errMsg:
        debug += "<BR>***%s***" % str(errMsg)
        tbl = debug
    except Exception:
        # if general eror, show traceback 
        tbl = debug + "<PRE>" + traceback.format_exc() + "</PRE>"        
    return tbl

def HTML_checklist(id, alist, adata='', onchg='', values=None, css='', multi=False, wrap=40):
    # used by the input_table_ajax routine to generate a list of checkboxes or radio buttons
    # id - ID used to id and name checkboxes/radio buttons
    # alist - list of texts for buttons
    # adata - current value(s), matching texts in a list.  Multiple values sparated by ;
    # onchg - onchange event to be attached to every button
    # values - if present, used as button value, otherwise the list text is the value.
    # css - css class applied to a DIV wrapping all the HTML
    # multi - if true, multi-select checkboxes created, otherwise single select radio buttons
    # wrap - lines wrapped with <BR> if next box+text would exceed 'wrap' characters per line
    htm = "<DIV class=%s>" % css if css>'' else "<DIV>"
    ch_count = 0    # counts characters per line, to trigger wrap when necessary
    firstLine = True    # flag to avoid line break before first line
    if not type(alist) is list:
        raise RuntimeError("Missing options list for selection (id=%s)" % id)
    # loop through list elements
    for i in range(len(alist)):
        # item is checked if a match with the data (NB possible false matches if one item is subset of another)
        chk = 'checked' if adata.find(alist[i]) >= 0 else ''                
        bt =  'checkbox' if multi else 'radio'       
        if type(values) is list:
            v=values[i]
        else:
            v=i    
        # html for button + text 
        btn = "<INPUT type=%s name=%s value='%s' title='%s' onchange='%s' %s>" % (bt, id, v, alist[i], onchg, chk)
        # do a line break except for first line          
        if firstLine: 
            firstLine = False
        else:
            htm += '<BR>'    
        # add button and text to HTML
        htm += btn + "&nbsp;" + alist[i]
        #ch_count += nch
     # close off DIV  and return the HTML
    htm += "</DIV>"
    return htm         
        
def f500_input_ajax(sessid, flid, cell, new_data):
    # processes changes to f500_input form via ajax requests.
    #
    # check session is open and get permission flag for this user
    reply = f500_edit_check(sessid)
    if reply['valid'] == 1:
        f500_audit_save(sessid, flid, cell, new_data)
        #params = "<P>sessid = %s<BR>flid = %s<BR>cell = %s<BR>new_data = %s</P>" % (sessid, flid, cell, new_data)
        #if 'debug' in reply:
        #    reply['debug'] += params
        #else:
        #    reply['debug'] = params 
        #reply['valid'] = 0                  
    return reply   

def f500_edit_check(sessid):
    # checks that a session has valid permission to edit the f500 input form
    # returns dictionary (JSON object) with 'valid'=1 if OK, 0 if not.  'msg' gives a message text if not OK.
    qry = getCursor()
    sqlcmd = qry.mogrify("""SELECT permit, email FROM gcdz.logins INNER JOIN gcdz.users USING (userid) 
        WHERE sessionid=%s AND timeout IS NULL""", (sessid,))
    qry.execute(sqlcmd)
    if qry.rowcount==0:
        msg = "Session ID '%s' is expired or not found" % sessid
        reply = {'valid': 0, 'msg': msg, 'debug': str(sqlcmd)}
    else:
        # check permit for this user/session is OK for this module (f500b)
        record = qry.fetchone()
        qry.execute("""SELECT id FROM gcdz.menus WHERE MODULE='f500b' AND (permitflag & %s)>0""", (record['permit'],))
        if qry.rowcount==0:
            reply = {'valid': 0, 'msg': "User '%s' not authorized for this module" % record['email']}
        else:
            # passed tests - session OK    
            reply = {'valid': 1}
    return reply

def f500_audit_save(sid, flid, cell, new_data):
    # used by data entry routines for forest 500 input form.  Saves data for a cell in xldata
    # for company identified by fileID
    # search for cell/flid in xldata
    qry = getCursor()
    qry.execute("""SELECT xlid, xltext FROM f500.xldata WHERE flid=%s AND xlcell=%s""", (flid, cell))
    if qry.rowcount==0:
        # cell-file ID combination not yet in system - add them
        qry.execute("INSERT INTO f500.xldata (flid, xlcell, xltext) VALUES(%s, %s, %s) RETURNING xlid", (flid, cell, new_data))
        # create record in audit table for new entry
        record = qry.fetchone()
        xlid = record['xlid']
        qry.execute("INSERT INTO f500.xldata_audit (xlid, sid, ts, newval) VALUES(%s, %s, NOW(), %s)", (xlid, sid, new_data))
    else:            
        # create record in audit table for updated entry
        record = qry.fetchone()
        xlid = record['xlid']
        old_data = record['xltext']
        qry.execute("INSERT INTO f500.xldata_audit (xlid, sid, ts, oldval, newval) VALUES(%s, %s, NOW(), %s, %s)", \
            (xlid, sid, old_data, new_data))
        # update record in xldata
        qry.execute("UPDATE f500.xldata SET xltext=%s WHERE xlid=%s", (new_data, xlid)); 
    # update status record for this form to show it has been edited, and by whom         
    qry.execute("UPDATE f500.survey_status SET statid=2, sessionid=%s, lastupd=now() WHERE flid=%s AND statid in (0, 1)", \
        (sid, flid))

def f500_select_ajax(sessid, flid, cell_t, cell_s, optval, opttxt):
    # processes changes to f500_input form assessment selection and score via ajax.
    # This routine is based on f500_input_ajax() for a general text input field
    # sessid - session ID
    # flid - File ID
    # cell_t - xldata cell containing assessment text for selected option
    # cell_s - xldata cell containing score corresponding to assessment text for selected option
    # optval - score value from SELECT OPTION
    # opttxt - text (assessment) for SELECT OPTION
    # check session is open and get permission flag for this user
    reply = f500_edit_check(sessid)
    if reply['valid'] == 1:
        f500_audit_save(sessid, flid, cell_t, opttxt)
        f500_audit_save(sessid, flid, cell_s, optval)
        reply['score'] = optval
    return reply   

def f500_savelink_ajax(sessid, flid, cell, inid, refid, cid, link_text):
    # strips any HTML from 'link_text' and sees if text is a valid link
    # if the link is valid and references a PDF file, it will be added to the references table
    # check session is valis
    reply = f500_edit_check(sessid)
    if not reply['valid'] :
        reply['ok'] = 0
        return reply    # also includes msg and debug fields from f500_edit_check
    # session is valid and authorised - first strip any HTML from link_text
    link = re.sub('<(.|\n)+?>', '', link_text)    
    # test the link to see if it connects
    try:
        fh = urlopen(link)
    except Exception as errMsg:
        # an error occurred opening the link
        reply['ok']=0
        reply['msg'] = str(errMsg)
        reply['debug'] = "<PRE>\n\n" +  traceback.format_exc() + "\n</PRE>"
        return reply
    # add the link to the table of references if it is new 
    qry = getCursor()
    qry.execute("SELECT docid FROM f500.documents WHERE flid=%s AND inid=%s AND sid=%s AND cid=%s AND url=%s", \
        (flid, inid, refid, cid, link))
    if qry.rowcount==0:    
        qry.execute("""INSERT INTO f500.documents(flid, inid, sid, cid, cell, url, sessid, ts) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, NOW()) RETURNING docid""", (flid, inid, refid, cid, cell, link, sessid))
        # add link as data in xldata table
        f500_audit_save(sessid, flid, cell, link)
    if qry.rowcount>=1:  
        docid =  qry.fetchone()[0]
        reply['ok'] = 1    
        reply['msg'] = "Saved : Document ID %s" % docid
        reply['link'] = link
    else:
        reply['ok']=0
        reply['msg'] = "Query to save data failed !! See debug data..."
        reply['debug'] = "<PRE>\n\n" +  str(qry.query) + "\n</PRE>"
    return reply

def f500_assess(sid, environ):
    # handles display and editing of Forest 500 assessments
    # show only top level here as a simple DIV.  All the detail is done via ajax (f500_ajax)
    postFields = getPostFields(environ)
    # name of calling script (exec or dev)
    scriptnm =  environ['SCRIPT_NAME']  
    # HTML for debugging info - left blank if none
    debug = ""
    fileid = postFields.get('FileID',[0])[0]
    js = '<SCRIPT src="https://www.gc-dz.com/js/f500.js"></SCRIPT>'
    # get company name
    qry = getCursor()
    qry.execute("""SELECT d.cotype, d.ayear, x.coname, x.flid, f.flname, DATE(f.fltime) FROM f500.xlcohdr AS x 
                INNER JOIN f500.filelist AS f ON x.flid=f.flid INNER JOIN f500.dirtree AS d ON f.dtid=d.dtid
                WHERE f.flid=%s """, (fileid,))
    if qry.rowcount==0:
        # show error heading, message and Close button
        html =  HTML_header(title= "Forest 500 Assessment - Error", width=1000, css="f500", extras=js, menulink = (sid, scriptnm))
        html += "<P class=error style='display: block'>Company ID not specified</P>" 
        html += "<p>&nbsp;</p><INPUT type=button value='Close' onclick='window.close()'>"
        html += "</BODY></HTML>"    
        return html
    # get company details and construct normal heading    
    co = qry.fetchone()
    title = "Forest 500 Assessment %s : %s" % (co['ayear'], co['coname'])
    html =  HTML_header(title, width=1000, css="f500", extras=js, menulink = (sid, scriptnm))
    # FORM action and hidden fields used if page is submitted
    #html += "<INPUT type=hidden name=FileID id=FileID value=%s>" % fileid
    # get names of same company in different years for the dropdown slector
    qry.execute("""SELECT CONCAT(x.coname, ' {', d.ayear, '}') AS coname_yr, x.flid::TEXT FROM f500.coflids AS c
    	INNER JOIN f500.xlcohdr AS x ON c.flid=x.flid
    	INNER JOIN f500.filelist AS f ON x.flid=f.flid 
    	INNER JOIN f500.dirtree AS d ON f.dtid=d.dtid 
    	WHERE ucid IN (SELECT ucid FROM f500.coflids WHERE flid=%s)
    	ORDER BY 2 """, (fileid,))    
    # company-year selector
    html += "<FORM action='%(APP)s?m=f500a&u=%(SID)s' id=form1 method=POST>" % {'APP': scriptnm, 'SID': sid}
    html += HTML_select("Company and Year", 50, "FileID", qry, fileid, "onchange=\"$('#form1').submit()\"")
    if qry.rowcount==0:
        html += "<P><SMALL>Company not listed in COFLIDS table!</SMALL></P>"
    html += "<BR clear=all>"
    #debug += "<P>Query:<BR>%s</P>" % qry.query    
    #debug += "<P>Query returned %s rows</P>" % qry.rowcount
    # match in companies table  
    # ---- to do ----
    # get commodities assessed as a list
    qry.execute("""SELECT c.cid, xltext AS asmt FROM f500.xldata AS x 
    	INNER JOIN f500.filelist AS f ON x.flid=f.flid 
    	INNER JOIN f500.dirtree AS d ON f.dtid=d.dtid 
    	INNER JOIN f500.cscore_cells AS c ON c.cass=x.xlcell AND c.ayear=d.ayear 
    	WHERE d.cotype='CO' AND x.flid=%s ORDER BY 1
        """, (fileid,))
    # covert retrieved records into a list of assessed commodities, by index number 
    if qry.rowcount>0:
        cdlist = []
        for cd in qry:
            if re.search('^([Yy]|1)',cd['asmt']):
                cdlist.append(cd['cid'])
    else:
        cdlist = None            
    # main indicator groups as a set of buttons
    qry.execute("""SELECT igid, heading FROM f500.ind_groups WHERE ayear=%s AND cotype=%s 
        ORDER BY igid""", (co['ayear'], co['cotype']))
    html += "<TABLE class=ind-tbl>"
    html += """<THEAD><TR>
            <TH style="width:  50px;">&nbsp;</TH>
            <TH style="width: 600px;">&nbsp;</TH>
            <TH style="width: 250px;">&nbsp;</TH>
            </TR></THEAD>"""
    html += "<TBODY>"   
    groups = qry.fetchall()
    for gp in groups:
        name= "IG%s" % gp['igid']
        js = 'toggleIndGroup(%s)' % gp['igid']
        btn_text = HTML_clean(gp['heading'])
        btn_val = HTML_clean(gp['igid'])
        html += """<TR class=btn-row><TD colspan=3>%s</TD></TR>""" % HTML_button(name, btn_text, btn_val,'ind-gp-btn', js)     
        # retrieve indicator details for this group
        qry.execute("""SELECT DISTINCT inid, indgrp, indnum, indtext, guide, scoring, maxpts FROM f500.ind_main 
            WHERE inid/100 = %s ORDER BY 1""",(int(gp['igid']),))
        indlist = qry.fetchall()
        for ind in indlist:
            # generate HTML for main indicator description
            (htm, dbg) =  HTML_indMain(ind, fileid, cdlist)
            html += htm
            debug += dbg
    # finish off table layout
    html += "</TBODY></TABLE>"             
    html += "</FORM>"
    # show window close button at bottom of page
    html += "<p>&nbsp;</p><INPUT type=button value='Close' onclick='window.close()'>"
    # pop-up notes DIV.  This positioned and populated in Javascript/ajax (see f500_indNotes).
    html += "<DIV id=ind-notes></DIV>"	
    # ---- debugging information 
    html += list_debug_info(postFields, debug, show=False)
    html += "</BODY></HTML>"    
    return html

def HTML_indMain(ind, fileid, cdlist):
    # creates the HTML for a main indicator
    # 'ind' has fields inid, indgrp, indnum, indtext, guide, scoring, maxpts
    # fileid is file being processed.  cdlist is a list of applicable commodity indices, or None 
    # if commodities not applicable to this file, ie a financial institution
    #
    debug=""
    inid = ind['inid']      # shorthand as field for Indicator ID is referenced repeatedly
    # first get rid of 'dangerous' characters in the text fields, but keep line breaks.
    text = HTML_clean(ind['indtext'], br=True)
    guide = HTML_clean(ind['guide'], br=True)
    scoring = HTML_clean(ind['scoring'], br=True)
    # now create the table stucture with the cleaned up text for the 'indicator' part of the first row
    html = """ <TR class=ind-row-A id=ind-A-%(INID)s >
            <TD onclick='toggleIndTable(%(INID)s)' >%(GP)s.%(NUM)s</TD>
            <TD onclick='toggleIndTable(%(INID)s)' >%(TEXT)s</TD>
        """  % {'INID': inid, 'GP': ind['indgrp'], 'NUM': ind['indnum'], 'TEXT': text}  
    # retrieve commodity and sub-indicators for this indicator id
    qry = getCursor()
    if cdlist is None:
        cdtext=""
    else:
        cdtext= " AND (i.cid = ANY (ARRAY%s) OR i.cid IS NULL) " % str(cdlist)
    query = """SELECT i.sid, i.cid, c.commodity, xltext AS score
    	FROM f500.ind_detail AS i 
    	LEFT JOIN f500.commodities AS c ON i.cid=c.cid 
    	LEFT JOIN f500.assmnt_parts AS p ON i.atype=p.atype 
    	LEFT JOIN f500.xldata AS x ON i.xlrow=x.xlrow AND p.xlcol=x.xlcol 
    	WHERE i.inid=%s AND x.flid=%s AND (p.ptype = 'S' OR p.ptype IS NULL) %s
    	ORDER BY 1, 2, 4""" % (inid, fileid, cdtext)	
    qry.execute(query)	
    #debug += "<P>Query:<BR>%s</P>" % query    
    #debug += "<P>Query returned %s rows</P>" % qry.rowcount
    if qry.rowcount==0:
        # query returned no rows - clean up html for this table row and return it
        html += "<TD>&nbsp;</TD></TR>"
        return (html, debug)
    # create first level sub-table to go in cell 3 of indicator row.  
    tbl1 =""
    # generate one row for each sid/commodity combination
    rows = qry.fetchall()
    br = ""         #line break tag, empty for first row
    id=0
    for row in rows:
        # texts for score, sid reference and commdity
        # scores in format 1, 1.5, 1.25 or - for false/None, 5 characters
        if row['score'] is None or str(row['score']).lower().strip()=='false':
            score = "&nbsp;-&nbsp;"
        else:    
            score = "{:5.3g}".format(float(row['score']))    
        # indicator cross- reference shown in brackets, if present    
        if row['sid'] is None:
            sid_txt = '&nbsp;' 
            sid='0'
        else:
            sid = str(row['sid'])           # change sid from 12345 to '3.45'
            sid_txt = "(%s.%s)" % (sid[2], sid[3:])  
        # commodity name                   
        if row['commodity'] is None:
            cd_txt = '&nbsp;' 
            cid='0'
        else:
           cd_txt = row['commodity']  
           cid = str(row['cid'])   
        id += 1   
        tbl1 += """%(BR)s<SMALL>%(CT)s&nbsp;%(ST)s&nbsp;<SMALL><B>%(SCORE)s</B>
                <IMG id=img%(INID)s-%(ID)s src='img/note_btn.png' border=0 height=10px width=10px 
                onclick="showIndDetails(%(ID)s, %(INID)s, %(CID)s, %(SID)s, %(FLID)s)">&nbsp;
                """  % {'BR': br, 'CT': cd_txt, 'ST': sid_txt, 'SCORE': score, \
                    'ID': id, 'INID': inid, 'CID': cid, 'SID': sid, 'FLID': fileid}  
        br = "<BR>&nbsp;"     # line break to preced next row (if any)   
    # add indicator details to last cell of row    
    html += "<TD style='text-align: right;'>" + tbl1 + "</TD></TR>"    
    # populate second row with additional text        
    html += """
        <TR class=ind-row-B id=ind-B-%(INID)s>            
            <TD colspan=2>%(GUIDE)s</TD>
            <TD colspan=1>%(SCOR)s</TD>
        </TR>
    """ % {'INID': inid, 'GUIDE': guide, 'SCOR': scoring}
    #debug += "<P>Final HTML:<BR> %s</P>" % HTML_clean(html)   
    return (html, debug)    

def f500_indNotes(inid, cid, sid, flid, imgid):
    # returns HTML to populate indicator notes popup 
    # see also javascript showIndDetails() in f500.js
    # and HTML_indMain where call to this function declared as onclick event
    # parameters are indicator id (inid), commodity index (cid), 'scope of' indicator (sid), 
    # file id (flid), and imgid, which is ID of the IMG button that called this routine
    #
    # construct WHERE sub-clauses if sid or cid are set
    wh_sid = " AND i.sid=%s " % sid if sid>'0' else ""
    wh_cid = " AND i.cid=%s " % cid if cid>'0' else ""
    # run query to collect notes for this indicator
    query = """SELECT p.ptid, p.ptlabel, p.ptype, x.xlcell, xltext 
    	FROM f500.ind_detail AS i 
    	INNER JOIN f500.assmnt_parts AS p ON i.atype=p.atype 
    	LEFT JOIN f500.xldata AS x ON i.xlrow=x.xlrow AND p.xlcol=x.xlcol 
    	WHERE i.inid=%(INID)s AND x.flid=%(FLID)s %(CID)s %(SID)s
    	ORDER BY 1, 2, 4""" % {'INID': inid, 'FLID': flid, 'CID': wh_cid, 'SID': wh_sid}
    qry = getCursor()
    qry.execute(query)	
    # close button at top right of DIV		
    html = """<IMG id=close src='img/delete.png' alt='Close Window' onclick='closeIndNotes("%s")'><BR clear=all>""" % imgid
    if qry.rowcount>0:
        # 3-column table for output
        html += "<TABLE>"
        # loop through data row
        rows = qry.fetchall()
        for row in rows:
        
            # wrap text with <BR> tags at 100 chars max.  
            text = HTML_lines(row['xltext'], width=100)
            # substitute any URLs with Anchor <A> tags
            text = HTML_link(text)
            # append to table
            html += "<TR><TD>%s</TD><TD>%s</TD><TD>%s</TD></TR>" % (row['xlcell'], row['ptlabel'], text)
        # close table
        html += "</TABLE>"        
    else:
        html += "<P>No notes found!</P>" % query
    return html

def HTML_link(text):
    # search for apparent hypertext links in text and wrap them in <A> tags pointing at the URL given
    # regex from JG Soft Library to match URLs, plus case-insensitive option
    urlrgx = re.compile('(?:(?:https?|ftp|file)://|www\.|ftp\.)[-A-Z0-9+&@#/%=~_|$?!:,.]*[A-Z0-9+&@#/%=~_|$]',re.I)
    # working copy of text 
    html = text
    # find all matching URL-like texts in target
    for murl in urlrgx.finditer(text):
        # create anchor link
        anchor = "<A href='%(URL)s' target=_blank class=note-link>%(URL)s</A>" % {'URL': murl.group()}
        html = html.replace(murl.group(), anchor)
    return html     
        
def HTML_clean(text, br=False):
    # replaces the characters &<>"' with HTML equivalents to avoid problems when injecting text into HTML    
    badch = '&<>"\''
    htmch = ['&amp;', '&lt;', '&gt;', '&quot;', '&#39;']
    txt = str(text)
    for i, ch in enumerate(badch):
        txt = re.sub(ch, htmch[i], txt)
    if br:
        # replace cr/lf with <BR>
        txt = re.sub('\r\n*', "<BR>", txt)           
    return txt

def HTML_lines(text, width=50):
    # converts a block of text or a long text line into a block of lines at most
    # 'width' chars long separated by <BR> tags.  Note this also cleans HTML of <>'" etc
    lines = textwrap.TextWrapper(width=width)
    text_lines = lines.wrap(text)
    # initialise HTML with first line of text
    html = text_lines[0]
    # add following lines preceded by line break.        
    for aline in text_lines[1:]:
        html += "<BR>" + aline
    return html    
        
def HTML_button(name, label, value, css="", onclick=""):
    # returns HTML for a button with text 'label', CSS 'class', javascript 'onclick'
    html = "<BUTTON name=%s id=%s value='%s' class=%s onclick='%s' type=button>%s</BUTTON>" % \
            (name, name, value, css, onclick, label)
    return html           

def list_debug_info(postFields=None, debug="", show=False):
    # list of POST fields for debugging.  'debug' can contain other debugging info.
    # by defualt , switch off.  show=True required to display listing.
    html = "<DIV id='debug'>"   # always create empty DIV.  Javascript may put messages here
    if show:
        html += "<BR><HR class=debug><BR>%s" % debug
        if postFields is not None:
            html += "POST fields:<BR>"
            # trap sensibly any errors looping through list of lists
            try:
                # list of POST fields
                for field in postFields.keys():
                    # each field is represented as a list of 1 or more items
                    for f in postFields[field]:
                        html += '%s = %s<BR>' % (field, f) 
            except Exception as e:
                # if error occurs in above, just show traceback
                html += traceback.format_exc()
    html += "</DIV>"   # end of debug div    
    return html


def HTML_select(hdr, sz, name, qry, dflt='', event=''):
# returns HTML for a SELECT dropdown.  'name' is the HTML name tag, qry is a cursor object with at least two fields,
# representing labels and values for the SELECT options. 'event' is text for an event routine such as "onclick = 'doSomeJs()'".
# The ID of the SELECT will be id+'name', which can be referenced in the sel_onchg() function.  The default option whose
# label is 'dflt' is check.  'hdr' and 'sz' are a heading for the drop down and size in monospaced characters.
# Note if the 'dflt' column is of integer type in the query, it should be caste to ::TEXT
    try:
        html = """
        <DIV class=datafield style="width: %(WIDTH)sch">
        <P>%(HDR)s</P>
        <SELECT name=%(NAME)s id=id%(NAME)s %(EVENT)s>
        """  % {'HDR': hdr, 'WIDTH': sz, 'NAME': name, 'EVENT': event}
        for opt in qry:
            selct = 'selected' if opt[1]==dflt else ''
            html += "<OPTION value='%s' %s>%s</option>" % (opt[1], selct, opt[0])
        html += "</SELECT></DIV>"    
    except Exception as e:
        # if error occurs in above show traceback + debug info       
        html = "<PRE>\n"
        html += "opt = %s\n\n" % str(opt)
        html += traceback.format_exc()
        html += "</PRE>"
    return html

def HTML_input(hdr, sz, name, dflt='', event=''):
# returns HTML for a TEXT INPUT.  'name' is the HTML name tag.'event' is text for an event routine such as "onclick = 'doSomeJs()'".
# The ID of the SELECT will be id+'name', which can be referenced in the sel_onchg() function.  The default value is 'dflt'.
# 'hdr' and 'sz' are a heading for the drop down and size in monospaced characters.
    try:
        html = """
        <DIV class=datafield style="width: %(WIDTH)sch">
        <P>%(HDR)s</P>
        <INPUT type=text name=%(NAME)s id=id%(NAME)s %(EVENT)s value="%(DFLT)s">
        </DIV> """  % {'HDR': hdr, 'WIDTH': sz, 'NAME': name, 'DFLT': dflt, 'EVENT': event}
    except Exception as e:
        # if error occurs in above show traceback + debug info       
        html = "<PRE>\n"
        html += "opt = %s\n\n" % str(opt)
        html += traceback.format_exc()
        html += "</PRE>"
    return html

def HTML_checkbox(hdr, sz, name, value='', chk=False, event=''):
# returns HTML for a CHECKBOX INPUT.  'name' is the HTML name tag.'event' is text for an event routine such as "onclick = 'doSomeJs()'".
# The ID of the SELECT will be id+'name', which can be referenced in the sel_onchg() function.  The value if checked is 'value'.
# If chk is True, box is marked as checked, otherwise not.
# 'hdr' and 'sz' are a heading for the drop down and size in monospaced characters.
    try:
       html = """
        <DIV class=datafield style="width: %(WIDTH)sch">
        <P>%(HDR)s</P>
        <INPUT type=checkbox name=%(NAME)s id=id%(NAME)s %(EVENT)s value="%(DFLT)s" %(CHK)s>
        </DIV> """  % {'HDR': hdr, 'WIDTH': sz, 'NAME': name, 'DFLT': value, 'EVENT': event, 'CHK': 'checked' if chk else ''}
    except Exception as e:
        # if error occurs in above show traceback + debug info       
        html = "<PRE>\n"
        html += "opt = %s\n\n" % str(opt)
        html += traceback.format_exc()
        html += "</PRE>"
    return html

    
def nameCheck(nmlist, chktype):
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

def HTML_header(title = "Data Manager Development Site", css="main", extras="", params ="", 
    width=600, menulink=None, module=""):
# standard HTML header for each page.  
# Title is inserted both as a meta tag and as a title at the top of the visible page, beside the GC logo. 
# module if present, gets a default title from the menu system for this module.  title is ignored if module is given.
# menulink should be a tuple of (sid, scriptnm) for a menu return link on the GC logo
# extras are HTML tags for the <HEAD> section, scripts, links, etc.
# css is the style sheet
# params are attributes for the <BODY> tag
    if menulink is None:
        # do nothing if menu link not given
        menu_url = ''
    else:
        # menulink must be a tuple (sid, scriptnm) otherwise an error will occur here
        (sid, scriptnm) = menulink
        menu_url = 'https://www.gc-dz.com/%s?m=menu&u=%s' % (scriptnm, sid)   
    if module>"":
        # get menu text for this module as page title
        qry = getCursor()
        qry.execute("SELECT menutext FROM gcdz.menus WHERE module=%s", (module,))
        mnu = qry.fetchone()
        title = mnu['menutext']             
    template = """
    <!DOCTYPE html>
    <HTML>
    <HEAD>
    <TITLE>Trasepad - %(TITLE)s</TITLE>
    <LINK href="https://www.gc-dz.com/css/%(CSS)s.css" rel="stylesheet"  type="text/css">
    <SCRIPT src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></SCRIPT>
    <SCRIPT src="https://code.jquery.com/ui/1.12.1/jquery-ui.min.js"></SCRIPT>
    <META charset="UTF-8">
    %(EXTRAS)s
    </HEAD>
    <BODY %(PARAMS)s>
    <a name="Top" id="Top"></a>
    <DIV class=logo><a href="%(MENU_URL)s"><img src="https://www.gc-dz.com/img/gc.jpg"></a></DIV>
    <DIV class=title><H1>%(TITLE)s</H1></DIV>
    <P class=clear>&nbsp;</P>
    <HR class=menu style="width: %(WIDTH)spx">
    """
    # substitute parameters in template
    html = template % {'TITLE': title, 'EXTRAS': extras,  'PARAMS': params, 'WIDTH': width, 'CSS': css, 'MENU_URL': menu_url}
    return html

def HTML_footer(environ=None):
    # standard HTML footer with back|menu link adn debugging material
    # if no environment data, footer link simply replicates Back button     
    if environ is None:
        html = """
        <p>&nbsp;</p>
        <p style="text-decoration: underline; color: green; cursor: pointer;" onclick="window.history.back()" title="Back to previous page">Back</p>
        </BODY>
        </HTML>    
        """ 
    else:
        # retrieve session ID and script name, then give return link to menu page
        params = cgi.parse_qs(environ['QUERY_STRING']) 
        sid = params.get('u', ['0'])[0]
        scriptnm =  environ['SCRIPT_NAME']  
        html = """
        <p>&nbsp;</p>
        <A href="https://www.gc-dz.com/%(APP)s?m=menu&u=%(SID)s" title="Return to menu">Menu</A>
        </BODY>
        </HTML>    
        """ % {'SID': sid, 'APP': scriptnm}
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

    

    
    
    
    
    
