###################################################################
# CAPTURE: CAsa Pipeline-cum-Toolkit for Upgraded GMRT data REduction
###################################################################
# Pipeline for analysing data from the GMRT and the uGMRT.
# Combination of pipelines done by Ruta Kale based on pipelines developed independently by Ruta Kale 
# and Ishwar Chandra.
# Date: 8th Aug 2019
# README : Please read the following instructions to run this pipeline on your data
# Files and paths required
# 0. This files from git should be placed and executed in the directory where your data files are located.
# 1. If starting from lta file, please provide the paths to the listscan and gvfits executable binaries in "gvbinpath" as shown.
# 2. Keep the vla-cals.list file in the same area.
# Please email ruta@ncra.tifr.res.in if you run into any issue and cannot solve.
# 



import logging
import os
import math
from datetime import datetime
logfile_name = datetime.now().strftime('capture_%H_%M_%S_%d_%m_%Y.log')
logging.basicConfig(filename=logfile_name,level=logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

logging.info("#######################################################################################")
logging.info("You are using CAPTURE: CAsa Pipeline-cum-Toolkit for Upgraded GMRT data REduction.")
logging.info("This has been developed at NCRA by Ruta Kale and Ishwara Chandra.")
logging.info("#######################################################################################")
logging.info("LOGFILE = %s", logfile_name)
logging.info("CASA_LOGFILE = %s", 'casa-'+logfile_name)
logging.info("#######################################################################################")

CASA_logfile = 'casa-'+logfile_name
casalog.setlogfile(CASA_logfile)

import ConfigParser
config = ConfigParser.ConfigParser()
config.read('config_capture.ini')


fromlta = config.getboolean('basic', 'fromlta')
fromfits = config.getboolean('basic', 'fromfits')
frommultisrcms = config.getboolean('basic','frommultisrcms')
findbadants = config.getboolean('basic','findbadants')                          
flagbadants= config.getboolean('basic','flagbadants')                      
findbadchans = config.getboolean('basic','findbadchans')                         
flagbadfreq= config.getboolean('basic','flagbadfreq')                           
flaginit = config.getboolean('basic','flaginit')                             
doinitcal = config.getboolean('basic','doinitcal')                              
doflag = config.getboolean('basic','doflag')                              
redocal = config.getboolean('basic','redocal')
dopolcal = config.getboolean('basic','dopolcal')                              
dosplit = config.getboolean('basic','dosplit')                               
flagsplitfile = config.getboolean('basic','flagsplitfile')                            
dosplitavg = config.getboolean('basic','dosplitavg')                             
doflagavg = config.getboolean('basic','doflagavg')                             
makedirty = config.getboolean('basic','makedirty')                            
doselfcal = config.getboolean('basic','doselfcal') 
dosubbandselfcal = config.getboolean('basic','dosubbandselfcal')
domidselfcal = config.getboolean('basic','domidselfcal')
dopolimages_target = config.getboolean('basic','dopolimages_target')
dosplit_calibrator = config.getboolean('basic','dosplit_calibrator')
usetclean = config.getboolean('default','usetclean')                        
ltafile =config.get('basic','ltafile')
gvbinpath = config.get('basic', 'gvbinpath').split(',')
fits_file = config.get('basic','fits_file')
msfilename =config.get('basic','msfilename')
splitfilename =config.get('basic','splitfilename')
splitavgfilename = config.get('basic','splitavgfilename')
selfcalvis = config.get('basic','selfcalvis')
scalsrno = int(config.get('basic','scalsrno'))
finalvis = config.get('basic','finalvis')
setquackinterval = config.getfloat('basic','setquackinterval')
ref_ant = config.get('basic','ref_ant')
clipfluxcal = [float(config.get('basic','clipfluxcal').split(',')[0]),float(config.get('basic','clipfluxcal').split(',')[1])]
clipphasecal =[float(config.get('basic','clipphasecal').split(',')[0]),float(config.get('basic','clipphasecal').split(',')[1])]
cliptarget =[float(config.get('basic','cliptarget').split(',')[0]),float(config.get('basic','cliptarget').split(',')[1])]   
clipresid=[float(config.get('basic','clipresid').split(',')[0]),float(config.get('basic','clipresid').split(',')[1])]
chanavg = config.getint('basic','chanavg')
subbandchan = config.getint('basic','subbandchan')
imcellsize = [config.get('basic','imcellsize')]
imsize_pix = int(config.get('basic','imsize_pix'))
clean_robust = float(config.get('basic','clean_robust'))
scaloops = config.getint('basic','scaloops')
mJythreshold = float(config.get('basic','mJythreshold'))
pcaloops = config.getint('basic','pcaloops')
scalsolints = config.get('basic','scalsolints').split(',')
niter_start = int(config.get('basic','niter_start'))
use_nterms = config.getint('basic','use_nterms')
nwprojpl = config.getint('basic','nwprojpl')
uvracal=config.get('default','uvracal')
uvrascal=config.get('default','uvrascal')
target = config.getboolean('default','target')


execfile('ugfunctions.py')

testfitsfile = False

if fromlta == True:
	logging.info("You have chosen to convert lta to FITS.")
	testltafile = os.path.isfile(ltafile)
	if testltafile == True:
		logging.info("The lta %s file exists.", ltafile)
		testlistscan = os.path.isfile(gvbinpath[0])
		testgvfits = os.path.isfile(gvbinpath[1])
		if testlistscan and testgvfits == True:
			os.system(gvbinpath[0]+' '+ltafile)
                        if fits_file!= '' and fits_file != 'TEST.FITS':
                                os.system("sed -i 's/TEST.FITS/'"+fits_file+"/ "+ltafile.split('.')[0]+'.log')
                        try:
                                assert os.path.isfile(fits_file)
                                testfitsfile = True
                        except AssertionError:
                                if os.path.isfile('TEST.FITS') == True: 
#                                assert os.path.isfile('TEST.FITS'), 
                                        logging.info("The file TEST.FITS file already exists. New will not be created.")
                                        testfitsfile = True
                                        fits_file = 'TEST.FITS'
                                else:
		                        os.system(gvbinpath[1]+' '+ltafile.split('.')[0]+'.log')
                                        testfitsfile = True
    		else:	
			logging.info("Error: Check if listscan and gvfits are present and executable.")
	else:
		logging.info("The given lta file does not exist. Exiting the code.")
		logging.info("If you are not starting from lta file please set fromlta to False and rerun.")
		sys.exit()


#testfitsfile = False 

if fromfits == True:
        if fits_file != '':
                try:
                        assert os.path.isfile(fits_file)
                        testfitsfile = True
                except AssertionError:
                        try:
                                assert os.path.isfile('TEST.FITS')
                                testfitsfile = True
                        except AssertionError:
                                logging.info("Please provide the name of the FITS file.")
                                sys.exit()

		

if testfitsfile == True:
        if msfilename != '':
                try:
                        assert os.path.isdir(msfilename), "The given msfile already exists, will not create new."
                except AssertionError:
                        logging.info("The given msfile does not exist, will create new.")
        else:
                try:
                        assert os.path.isdir(fits_file+'.MS')
                except AssertionError:
                        msfilename = fits_file+'.MS'           
	default(importgmrt)
	importgmrt(fitsfile=fits_file, vis = msfilename)
	if os.path.isfile(msfilename+'.list') == True:
		os.system('rm '+msfilename+'.list')
	vislistobs(msfilename)
        logging.info("Please see the text file with the extension .list to find out more about your data.")
	

testms = False

if frommultisrcms == True:
        if msfilename != '':
	        testms = os.path.isdir(msfilename)
        else:
                try:
                        assert os.path.isdir('TEST.FITS.MS')
                        testms = True
                        msfilename = 'TEST.FITS.MS'
                except AssertionError:
                        logging.info("Tried to find the MS file with default name. File not found. Please provide the name of the msfile or create the MS by setting fromfits = True.")
                        sys.exit()
	if testms == False:
		logging.info("The MS file does not exist. Please provide msfilename. Exiting the code...")
                sys.exit()


if testms == True:
        gainspw, mygoodchans, flagspw, mypol = getgainspw(msfilename)
	logging.info("Channel range for calibration:")
        logging.info(gainspw)
	logging.info("Assumed clean channel range:")
        logging.info(mygoodchans)
	logging.info("Channel range for flagging:")
        logging.info(flagspw)
	logging.info("Polarizations in the file:")
        logging.info(mypol)
# fix targets
        myfields = getfields(msfilename)
        stdcals = ['3C48','3C147','3C286','0542+498','1331+305','0137+331']
        stdpolcals = ['3C286','3C138']
        stdunpolcals = ['3C84']
        stdothercal = ['OQ208']
        vlacals = np.loadtxt('./vla-cals.list',dtype='string')
        myampcals =[]
        mypcals=[]
        mypolcals =[]
        myunpolcals =[]
        mytargets=[]
        myocals =[]
        for i in range(0,len(myfields)):
                if myfields[i] in stdcals:
                        myampcals.append(myfields[i])
                elif myfields[i] in vlacals:
                        mypcals.append(myfields[i])
                else:
                        mytargets.append(myfields[i])
                if myfields[i] in stdpolcals:
                        mypolcals.append(myfields[i])
                if myfields[i] in stdunpolcals:
                        myunpolcals.append(myfields[i])
                if myfields[i] in stdothercal:
                        myocals.append(myfields[i])
	try: 
		mytargets.remove('3C84')
	except ValueError:
		logging.info("3C84 is not in the target list.")
	try:
		mytargets.remove('OQ208')
	except ValueError:
		logging.info("OQ208 is not in the target list.")
        mybpcals = myampcals
        if '3C138' in mypolcals:
                mytransfield  = mypcals+myunpolcals+myocals+['3C138']
        else:
                mytransfield =mypcals+myunpolcals+myocals
        logging.info('Amplitude caibrators are %s', str(myampcals))
        logging.info('Phase calibrators are %s', str(mypcals))
        logging.info('Polarized calibrators are %s', str(mypolcals))
        logging.info('Un-polarized calibrators are %s', str(myunpolcals))
        logging.info('Other calibrator is %s',str(myocals))
        logging.info('Target sources are %s', str(mytargets))
        logging.info('Transfer fields are %s', str(mytransfield))
#on to see if the pcal is same as 
        ampcalscans =[]
	nscan_nos=[]
        for i in range(0,len(myampcals)):
                ampcalscans.extend(getscans(msfilename, myampcals[i]))
		nscan_nos.append(len(getscans(msfilename, myampcals[i])))
		logging.info('Scans for %s are %s', str(myampcals[i]), str(len(getscans(msfilename, myampcals[i]))))
	max_nscans= max(nscan_nos)
	max_index= nscan_nos.index(max_nscans)
        logging.info('Amp cal scans are %s', str(ampcalscans))
	pampcal = myampcals[max_index]
	logging.info('The amp calibrator %s may be used as pcal if needed', myampcals[max_index])
        pcalscans=[]
        for i in range(0,len(mypcals)):
                pcalscans.extend(getscans(msfilename, mypcals[i]))
        logging.info('Phase cal scans are %s', str(pcalscans))
# other calibrators will be clubbed with pcalscans for flagging purpose
        for i in range(0,len(mypolcals)):
                pcalscans.extend(getscans(msfilename, mypolcals[i]))
        logging.info('Phase cal scans and polcals are %s', str(pcalscans))
        for i in range(0,len(myunpolcals)):
                pcalscans.extend(getscans(msfilename, myunpolcals[i]))
        logging.info('Phase cal scans and polcals and unpolcals are %s', str(pcalscans))
        for i in range(0,len(myocals)):
                pcalscans.extend(getscans(msfilename, myocals[i]))
        logging.info('Phase cal scans and polcals and unpolcals and ocals are %s', str(pcalscans))
        tgtscans=[]
        for i in range(0,len(mytargets)):
                tgtscans.extend(getscans(msfilename,mytargets[i]))
        logging.info('Target scans are %s', str(tgtscans))
        if '3C286' in myampcals:
                polcalib ='3C286' # hardcoded for now
        else:
                polcalib = myampcals[0]
        logging.info('Pol cal is  %s', str(polcalib))
        mycals=myampcals+mypcals
        myrestcals = mypolcals+myunpolcals+myocals
        myrestcals.remove('3C286')
        myallcals = myrestcals+mycals
#####################
# fix targets
#	myfields = getfields(msfilename)
#	stdcals = ['3C48','3C147','3C286','0542+498','1331+305','0137+331']
#	vlacals = np.loadtxt('./vla-cals.list',dtype='string')
#	myampcals =[]
#	mypcals=[]
#	mytargets=[]
#	for i in range(0,len(myfields)):
#		if myfields[i] in stdcals:
#			myampcals.append(myfields[i])
#		elif myfields[i] in vlacals:
#			mypcals.append(myfields[i])
#		else:
#			mytargets.append(myfields[i])
#	mybpcals = myampcals
#	logging.info('Amplitude caibrators are %s', str(myampcals))
#	logging.info('Phase calibrators are %s', str(mypcals))
#	logging.info('Target sources are %s', str(mytargets))
# need a condition to see if the pcal is same as 
#	ampcalscans =[]
#	for i in range(0,len(myampcals)):
#		ampcalscans.extend(getscans(msfilename, myampcals[i]))
#	pcalscans=[]
#	for i in range(0,len(mypcals)):
#		pcalscans.extend(getscans(msfilename, mypcals[i]))
#	tgtscans=[]
#	for i in range(0,len(mytargets)):
#		tgtscans.extend(getscans(msfilename,mytargets[i]))
##	print(ampcalscans)
#	logging.info("Amplitude calibrator scans are:")
#        logging.info(ampcalscans)
##	print(pcalscans)
#	logging.info("Phase calibrator scans are:")
#        logging.info(pcalscans)
##	print(tgtscans)
#	logging.info("Target source scans are:")
#        logging.info(tgtscans)
	allscanlist= ampcalscans+pcalscans+tgtscans
###################################
# get a list of antennas
	antsused = getantlist(msfilename,int(allscanlist[0]))
	logging.info("Antennas in the file:")
        logging.info(antsused)
###################################
# find band ants
	if flagbadants==True:
		findbadants = True
	if findbadants == True:
		myantlist = antsused
		mycmds = []
#############
		meancutoff = getbandcut(msfilename)
#############
		mycorr1='rr'
		mycorr2='ll'
		mygoodchans1=mygoodchans
		mycalscans = ampcalscans+pcalscans
#		print(mycalscans)
		logging.info("Calibrator scan numbers:")
		logging.info(mycalscans)
		allbadants=[]
		for j in range(0,len(mycalscans)):
			myantmeans = []
			badantlist = []
			for i in range(0,len(myantlist)):
                                if mypol == 1:
                                        if poldata == 'RR':
                                                oneantmean1 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr1,str(mycalscans[j]))
                                                oneantmean2 =oneantmean1*100.
                                        elif poldata == 'LL':
                                                oneantmean2 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr2,str(mycalscans[j]))
                                                oneantmean1=oneantmean2*100.
                                else:
                                        oneantmean1 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr1,str(mycalscans[j]))
                                        oneantmean2 = myvisstatampraw(msfilename,mygoodchans,myantlist[i],mycorr2,str(mycalscans[j]))
				oneantmean = min(oneantmean1,oneantmean2)
				myantmeans.append(oneantmean)
				if oneantmean < meancutoff:
					badantlist.append(myantlist[i])
					allbadants.append(myantlist[i])
			logging.info("The following antennas are bad for the given scan numbers.")
			logging.info('%s, %s',str(badantlist), str(mycalscans[j]))
			if badantlist!=[]:
				myflgcmd = "mode='manual' antenna='%s' scan='%s'" % (str('; '.join(badantlist)), str(mycalscans[j]))
				mycmds.append(myflgcmd)
				logging.info(myflgcmd)
				onelessscan = mycalscans[j] - 1
				onemorescan = mycalscans[j] + 1
				if onelessscan in tgtscans:
					myflgcmd = "mode='manual' antenna='%s' scan='%s'" % (str('; '.join(badantlist)), str(mycalscans[j]-1))
					mycmds.append(myflgcmd)
					logging.info(myflgcmd)
				if onemorescan in tgtscans:
					myflgcmd = "mode='manual' antenna='%s' scan='%s'" % (str('; '.join(badantlist)), str(mycalscans[j]+1))
					mycmds.append(myflgcmd)
					logging.info(myflgcmd)
# execute the flagging commands accumulated in cmds
		if flagbadants==True:
			logging.info("Now flagging the bad antennas.")
			default(flagdata)
			flagdata(vis=msfilename,mode='list', inpfile=mycmds)	
######### Bad channel flagging for known persistent RFI.
	if flagbadfreq==True:
		findbadchans = True
	if findbadchans ==True:
		rfifreqall =[0.36E09,0.3796E09,0.486E09,0.49355E09,0.8808E09,0.885596E09,0.7646E09,0.769092E09] # always bad
		myfreqs =  freq_info(msfilename)
		mybadchans=[]
		for j in range(0,len(rfifreqall)-1,2):
			for i in range(0,len(myfreqs)):
				if (myfreqs[i] > rfifreqall[j] and myfreqs[i] < rfifreqall[j+1]): #(myfreqs[i] > 0.486E09 and myfreqs[i] < 0.49355E09):
					mybadchans.append('0:'+str(i))
		mychanflag = str(', '.join(mybadchans))
		if mybadchans!=[]:
			myflgcmd = ["mode='manual' spw='%s'" % (mychanflag)]
			if flagbadfreq==True:
				default(flagdata)
				flagdata(vis=msfilename,mode='list', inpfile=myflgcmd)
		else:
			logging.info("None of the well-known RFI-prone frequencies were found in the data.")
############ Initial flagging ################
if flaginit == True:
        try:
                assert os.path.isdir(msfilename), "flaginit = True but ms file not found."
        except AssertionError:
                logging.info("flaginit = True but ms file not found.")
                sys.exit()
	casalog.filter('INFO')
#Step 1 : Flag the first channel.
	default(flagdata)
	flagdata(vis=msfilename, mode='manual', field='', spw='0:0', antenna='', correlation='', action='apply', savepars=True,
		cmdreason='badchan', outfile='')
#Step 3: Do a quack step 
	default(flagdata)
	flagdata(vis=msfilename, mode='quack', field='', spw='0', antenna='', correlation='', timerange='',
		quackinterval=setquackinterval, quackmode='beg', action='apply', savepars=True, cmdreason='quackbeg',
	        outfile='')
	default(flagdata)
	flagdata(vis=msfilename, mode='quack', field='', spw='0', antenna='', correlation='', timerange='', quackinterval=setquackinterval,
		quackmode='endb', action='apply', savepars=True, cmdreason='quackendb', outfile='')
# Clip at high amp levels
	if myampcals !=[]:
		default(flagdata)
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(','.join(myampcals)), clipminmax=clipfluxcal, datacolumn="DATA",clipoutside=True, clipzeros=True, extendpols=False, 
        		action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
	if mypcals !=[]:
		default(flagdata)
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(','.join(mypcals)), clipminmax=clipphasecal, datacolumn="DATA",clipoutside=True, clipzeros=True, extendpols=False, 
        		action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
# After clip, now flag using 'tfcrop' option for flux and phase cal tight flagging
		flagdata(vis=msfilename,mode="tfcrop", datacolumn="DATA", field=str(','.join(mypcals)), ntime="scan",
		        timecutoff=5.0, freqcutoff=5.0, timefit="line",freqfit="line",flagdimension="freqtime", 
		        extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
		        action="apply", flagbackup=True,overwrite=True, writeflags=True)
# Now extend the flags (80% more means full flag, change if required)
		flagdata(vis=msfilename,mode="extend",spw=flagspw,field=str(','.join(mypcals)),datacolumn="DATA",clipzeros=True,
		         ntime="scan", extendflags=False, extendpols=True,growtime=80.0, growfreq=80.0,growaround=False,
		         flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,overwrite=True, writeflags=True)
######### target flagging ### clip first
	if target == True:
		if mytargets !=[]:
			flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(','.join(mytargets)), clipminmax=cliptarget, datacolumn="DATA",clipoutside=True, clipzeros=True, extendpols=False, 
		        	action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
# flagging with tfcrop before calibration
			default(flagdata)
			flagdata(vis=msfilename,mode="tfcrop", datacolumn="DATA", field=str(','.join(mytargets)), ntime="scan",
		        	timecutoff=6.0, freqcutoff=6.0, timefit="poly",freqfit="poly",flagdimension="freqtime", 
		        	extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
		        	action="apply", flagbackup=True,overwrite=True, writeflags=True)
# Now extend the flags (80% more means full flag, change if required)
			flagdata(vis=msfilename,mode="extend",spw=flagspw,field=str(','.join(mytargets)),datacolumn="DATA",clipzeros=True,
		        	 ntime="scan", extendflags=False, extendpols=True,growtime=80.0, growfreq=80.0,growaround=False,
		        	 flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,overwrite=True, writeflags=True)
# Now summary
		flagdata(vis=msfilename,mode="summary",datacolumn="DATA", extendflags=True, 
	        	 name=vis+'summary.split', action="apply", flagbackup=True,overwrite=True, writeflags=True)	
        logging.info("A flagging summary is provided for the MS file.")
        flagsummary(msfilename)
#####################################################################
# Calibration begins.
if doinitcal == True:
	assert os.path.isdir(msfilename)
        try:
                assert os.path.isdir(msfilename), "doinitcal = True but ms file not found."
        except AssertionError:
                logging.info("doinitcal = True but ms file not found.")
                sys.exit()
	mycalsuffix = ''
	casalog.filter('INFO')
	clearcal(vis=msfilename)
	for i in range(0,len(myampcals)):
		default(setjy)
		setjy(vis=msfilename, spw=flagspw, field=myampcals[i])
# Delay calibration  using the first flux calibrator in the list - should depend on which is less flagged
	if os.path.isdir(str(msfilename)+'.K1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.K1'+mycalsuffix)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.K1'+mycalsuffix, spw =flagspw, field=myampcals[0], 
		solint='60s', refant=ref_ant,	solnorm= True, gaintype='K', gaintable=[], parang=True)
	kcorrfield =myampcals[0]
# an initial bandpass
	if os.path.isdir(str(msfilename)+'.AP.G0'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G0'+mycalsuffix)
	default(gaincal)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.AP.G0'+mycalsuffix, append=True, field=str(','.join(mybpcals)), 
		spw =flagspw, solint = 'int', refant = ref_ant, minsnr = 2.0, solmode = 'L1R', gaintype = 'G', calmode = 'ap', gaintable = [str(msfilename)+'.K1'+mycalsuffix],
		interp = ['nearest,nearestflag', 'nearest,nearestflag' ], parang = True)
	if os.path.isdir(str(msfilename)+'.B1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.B1'+mycalsuffix)
	default(bandpass)
	bandpass(vis=msfilename, caltable=str(msfilename)+'.B1'+mycalsuffix, spw =flagspw, field=str(','.join(mybpcals)), solint='inf', refant=ref_ant, solnorm = True,
		minsnr=2.0, fillgaps=8, parang = True, gaintable=[str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.AP.G0'+mycalsuffix], interp=['nearest,nearestflag','nearest,nearestflag'])
# do a gaincal on all calibrators
	mycals=myampcals+mypcals
	myrestcals = mypolcals+myunpolcals+myocals
	myrestcals.remove('3C286')
	myallcals = myrestcals+mycals
	i=0
	if os.path.isdir(str(msfilename)+'.AP.G'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G'+mycalsuffix)
#	for i in range(0,len(mycals)):
#		mygaincal_ap2(msfilename,mycals[i],ref_ant,gainspw,uvracal,mycalsuffix)
        for i in range(0,len(myallcals)):
                mygaincal_ap2(msfilename,myallcals[i],ref_ant,gainspw,uvracal,mycalsuffix)	
# Get flux scale
	if os.path.isdir(str(msfilename)+'.fluxscale'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.fluxscale'+mycalsuffix)
######################################
#	if mypcals !=[]:
#		if '3C286' in myampcals:
#			myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(mypcals)),mycalsuffix)
#			myfluxscaleref = '3C286'
#		elif '3C147' in myampcals:
#			myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(mypcals)),mycalsuffix)
#			myfluxscaleref = '3C147'
#		else:
#			myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(mypcals)),mycalsuffix)
#			myfluxscaleref = myampcals[0]
#		logging.info(myfluxscale)
#		mygaintables =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
#	else:
#		mygaintables =[str(msfilename)+'.AP.G'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
##############################
	if mypcals !=[]:
                if '3C286' in myampcals:
                        myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(mypcals+myrestcals)),mycalsuffix)
                        myfluxscaleref = '3C286'
                elif '3C147' in myampcals:
                        myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(mypcals+myrestcals)),mycalsuffix)
                        myfluxscaleref = '3C147'
                else:
                        myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(mypcals+myrestcals)),mycalsuffix)
                        myfluxscaleref = myampcals[0]
                logging.info(myfluxscale)
                mygaintables =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
#                if '3C286' in myampcals:
#                        myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(mypcals+myrestcals)),mycalsuffix)
#                        myfluxscaleref = '3C286'
#                elif '3C147' in myampcals:
#                        myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(mypcals+myrestcals)),mycalsuffix)
#                        myfluxscaleref = '3C147'
#                else:
#                        myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(mypcals+myrestcals)),mycalsuffix)
#                        myfluxscaleref = myampcals[0]
#                logging.info(myfluxscale)
                mygaintables_rest =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]		
	else:
                mygaintables =[str(msfilename)+'.AP.G'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
		if myrestcals !=[]:
	                if '3C286' in myampcals:
        	                myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(mypcals+myrestcals)),mycalsuffix)
                	        myfluxscaleref = '3C286'
	                elif '3C147' in myampcals:
        	                myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(mypcals+myrestcals)),mycalsuffix)
                	        myfluxscaleref = '3C147'
	                else:
        	                myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(mypcals+myrestcals)),mycalsuffix)
                	        myfluxscaleref = myampcals[0]
	                logging.info(myfluxscale)
			mygaintables_rest =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
##############################
	for i in range(0,len(myampcals)):
		default(applycal)
		applycal(vis=msfilename, field=myampcals[i], spw = flagspw, gaintable=mygaintables, gainfield=[myampcals[i],kcorrfield,''], 
        		 interp=['nearest','','nearest'], calwt=[False], parang=False)
		logging.info(mygaintables)
		gainfields = [myampcals[i],kcorrfield,'']
		logging.info(gainfields)
#For phase calibrator:
        if mypcals !=[]:
		for i in range(0,len(mypcals)):
	                default(applycal)
        	        applycal(vis=msfilename, field=mypcals[i], spw = flagspw, gaintable=mygaintables, gainfield=[mypcals[i],kcorrfield,myampcals],
                	         interp=['nearest','','nearest'], calwt=[False], parang=False)
			logging.info(mygaintables)
			gainfields = [mypcals[i],kcorrfield,myampcals]
			logging.info(gainfields)
#	if mypcals !=[]:
#		default(applycal)
#		applycal(vis=msfilename, field=str(', '.join(mypcals)), spw = flagspw, gaintable=mygaintables, gainfield=str(', '.join(mypcals)), 
#		         interp=['nearest','','nearest'], calwt=[False], parang=False)
###################
	if myrestcals !=[]:
                for i in range(0,len(myrestcals)):
	                default(applycal)
        	        applycal(vis=msfilename, field=myrestcals[i], spw = flagspw, gaintable=mygaintables_rest, gainfield=[myrestcals[i],kcorrfield,myampcals],
                	         interp=['nearest','','nearest'], calwt=[False], parang=False)		
                        logging.info(mygaintables_rest)
			gainfields = [myrestcals[i],kcorrfield,myampcals]
                        logging.info(gainfields)
			
###################
#For the target:
	if target ==True:
		if mypcals !=[]:
			default(applycal)
			applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
		        	 gainfield=[str(', '.join(mypcals)),'',''],interp=['linear','','nearest'], calwt=[False], parang=False)
                        logging.info(mygaintables)
			gainfields = [str(', '.join(mypcals)),'','']
                        logging.info(gainfields)
#		else:
#			default(applycal)
#			applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
#		        	 gainfield=[str(', '.join(myampcals)),'',''],interp=['linear','','nearest'], calwt=[False], parang=False)	
#                       logging.info(mygaintables)
#			gainfields = [str(', '.join(myampcals)),'','']
#                       logging.info(gainfields)
############new code to use flux cal with max scans as the pcal#####
		else:
			default(applycal)
			applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
		        	 gainfield=[pampcal,'',''],interp=['linear','','nearest'], calwt=[False], parang=False)	
                        logging.info(mygaintables)
			gainfields = [pampcal,'','']
                        logging.info(gainfields)
#################################################################
	logging.info("Finished initial calibration.")
        logging.info("A flagging summary is provided for the MS file.")
        flagsummary(msfilename)
#############################################################################3
#######Ishwar post calibration flagging
if doflag == True:
        try:
                assert os.path.isdir(msfilename), "doflag = True but ms file not found."
        except AssertionError:
                logging.info("doflag = True but ms file not found.")
                sys.exit()
	logging.info("You have chosen to flag after the initial calibration.")
        mycals=myampcals+mypcals
        myrestcals = mypolcals+myunpolcals+myocals
        myrestcals.remove('3C286')
        myallcals = myrestcals+mycals
	default(flagdata)
	if myampcals !=[]:
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(', '.join(myampcals)), clipminmax=clipfluxcal,
        		datacolumn="corrected",clipoutside=True, clipzeros=True, extendpols=False, 
        		action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
	if mypcals !=[]:
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(', '.join(mypcals)), clipminmax=clipphasecal,
        		datacolumn="corrected",clipoutside=True, clipzeros=True, extendpols=False, 
        		action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
# After clip, now flag using 'tfcrop' option for flux and phase cal tight flagging
		flagdata(vis=msfilename,mode="tfcrop", datacolumn="corrected", field=str(', '.join(mypcals)), ntime="scan",
        		timecutoff=6.0, freqcutoff=5.0, timefit="line",freqfit="line",flagdimension="freqtime", 
        		extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
        		action="apply", flagbackup=True,overwrite=True, writeflags=True)
# now flag using 'rflag' option  for flux and phase cal tight flagging
		flagdata(vis=msfilename,mode="rflag",datacolumn="corrected",field=str(', '.join(mypcals)), timecutoff=5.0, 
		        freqcutoff=5.0,timefit="poly",freqfit="line",flagdimension="freqtime", extendflags=False,
		        timedevscale=4.0,freqdevscale=4.0,spectralmax=500.0,extendpols=False, growaround=False,
		        flagneartime=False,flagnearfreq=False,action="apply",flagbackup=True,overwrite=True, writeflags=True)
# Now extend the flags (70% more means full flag, change if required)
		flagdata(vis=msfilename,mode="extend",spw=flagspw,field=str(', '.join(mypcals)),datacolumn="corrected",clipzeros=True,
		         ntime="scan", extendflags=False, extendpols=False,growtime=90.0, growfreq=90.0,growaround=False,
		         flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,overwrite=True, writeflags=True)
##########new block for restcals ########
#        if myrestcals !=[]:
#                flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(', '.join(myrestcals)), clipminmax=clipphasecal,
#                        datacolumn="corrected",clipoutside=True, clipzeros=True, extendpols=False,
#                        action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
# After clip, now flag using 'tfcrop' option for flux and phase cal tight flagging
#                flagdata(vis=msfilename,mode="tfcrop", datacolumn="corrected", field=str(', '.join(myrestcals)), ntime="scan",
#                        timecutoff=6.0, freqcutoff=5.0, timefit="line",freqfit="line",flagdimension="freqtime",
#                        extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
#                        action="apply", flagbackup=True,overwrite=True, writeflags=True)
# now flag using 'rflag' option  for flux and phase cal tight flagging # overflagging on 3C84 seen
#                flagdata(vis=msfilename,mode="rflag",datacolumn="corrected",field=str(', '.join(myrestcals)), timecutoff=5.0,
#                        freqcutoff=5.0,timefit="poly",freqfit="line",flagdimension="freqtime", extendflags=False,
#                        timedevscale=4.0,freqdevscale=4.0,spectralmax=500.0,extendpols=False, growaround=False,
#                        flagneartime=False,flagnearfreq=False,action="apply",flagbackup=True,overwrite=True, writeflags=True)
# Now extend the flags (70% more means full flag, change if required)
#                flagdata(vis=msfilename,mode="extend",spw=flagspw,field=str(', '.join(myrestcals)),datacolumn="corrected",clipzeros=True,
#                         ntime="scan", extendflags=False, extendpols=False,growtime=90.0, growfreq=90.0,growaround=False,
#                         flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,overwrite=True, writeflags=True)
########################################
# Now flag for target - moderate flagging, more flagging in self-cal cycles
	if mytargets !=[]:
		flagdata(vis=msfilename,mode="clip", spw=flagspw,field=str(', '.join(mytargets)), clipminmax=cliptarget,
		        datacolumn="corrected",clipoutside=True, clipzeros=True, extendpols=False, 
		        action="apply",flagbackup=True, savepars=False, overwrite=True, writeflags=True)
# C-C baselines are selected
		a, b = getbllists(msfilename)
		flagdata(vis=msfilename,mode="tfcrop", datacolumn="corrected", field=str(', '.join(mytargets)), antenna=a[0],
			ntime="scan", timecutoff=8.0, freqcutoff=8.0, timefit="poly",freqfit="line",flagdimension="freqtime", 
		        extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
		        action="apply", flagbackup=True,overwrite=True, writeflags=True)
# C- arm antennas and arm-arm baselines are selected.
		flagdata(vis=msfilename,mode="tfcrop", datacolumn="corrected", field=str(', '.join(mytargets)), antenna=b[0],
			ntime="scan", timecutoff=6.0, freqcutoff=5.0, timefit="poly",freqfit="line",flagdimension="freqtime", 
        		extendflags=False, timedevscale=5.0,freqdevscale=5.0, extendpols=False,growaround=False,
        		action="apply", flagbackup=True,overwrite=True, writeflags=True)
# now flag using 'rflag' option
# C-C baselines are selected
		flagdata(vis=msfilename,mode="rflag",datacolumn="corrected",field=str(', '.join(mytargets)), timecutoff=5.0, antenna=a[0],
        		freqcutoff=8.0,timefit="poly",freqfit="poly",flagdimension="freqtime", extendflags=False,
        		timedevscale=8.0,freqdevscale=5.0,spectralmax=500.0,extendpols=False, growaround=False,
        		flagneartime=False,flagnearfreq=False,action="apply",flagbackup=True,overwrite=True, writeflags=True)
# C- arm antennas and arm-arm baselines are selected.
		flagdata(vis=msfilename,mode="rflag",datacolumn="corrected",field=str(', '.join(mytargets)), timecutoff=5.0, antenna=b[0],
        		freqcutoff=5.0,timefit="poly",freqfit="poly",flagdimension="freqtime", extendflags=False,
        		timedevscale=5.0,freqdevscale=5.0,spectralmax=500.0,extendpols=False, growaround=False,
        		flagneartime=False,flagnearfreq=False,action="apply",flagbackup=True,overwrite=True, writeflags=True)
# Now summary
	flagdata(vis=msfilename,mode="summary",datacolumn="corrected", extendflags=True, 
         name=vis+'summary.split', action="apply", flagbackup=True,overwrite=True, writeflags=True)
        logging.info("A flagging summary is provided for the MS file.")
        flagsummary(msfilename)

#################### new redocal #########################3
# Calibration begins.
if redocal == True:
        try:
                assert os.path.isdir(msfilename), "redocal = True but ms file not found."
        except AssertionError:
                logging.info("redocal = True but ms file not found.")
                sys.exit()
	logging.info("You have chosen to redo the calibration on your data.")
	mycalsuffix = 'recal'
	casalog.filter('INFO')
	clearcal(vis=msfilename) ## commented for testing polcal part quickly
	for i in range(0,len(myampcals)):
		default(setjy)
		setjy(vis=msfilename, spw=flagspw, field=myampcals[i])
		logging.info("Done setjy on %s"%(myampcals[i]))
# Delay calibration  using the first flux calibrator in the list - should depend on which is less flagged
	if os.path.isdir(str(msfilename)+'.K1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.K1'+mycalsuffix)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.K1'+mycalsuffix, spw =flagspw, field=myampcals[0], 
		solint='60s', refant=ref_ant,	solnorm= True, gaintype='K', gaintable=[], parang=True)
	kcorrfield =myampcals[0]
#	print 'wrote table',str(msfilename)+'.K1'+mycalsuffix
# an initial bandpass
	if os.path.isdir(str(msfilename)+'.AP.G0'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G0'+mycalsuffix)
	default(gaincal)
	gaincal(vis=msfilename, caltable=str(msfilename)+'.AP.G0'+mycalsuffix, append=True, field=str(','.join(mybpcals)), 
		spw =flagspw, solint = 'int', refant = ref_ant, minsnr = 2.0, solmode ='L1R', gaintype = 'G', calmode = 'ap', gaintable = [str(msfilename)+'.K1'],
		interp = ['nearest,nearestflag', 'nearest,nearestflag' ], parang = True)
	if os.path.isdir(str(msfilename)+'.B1'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.B1'+mycalsuffix)
	default(bandpass)
	bandpass(vis=msfilename, caltable=str(msfilename)+'.B1'+mycalsuffix, spw =flagspw, field=str(','.join(mybpcals)), solint='inf', refant=ref_ant, solnorm = True,
		minsnr=2.0, fillgaps=8, parang = True, gaintable=[str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.AP.G0'+mycalsuffix], interp=['nearest,nearestflag','nearest,nearestflag'])
# do a gaingal on all calibrators
	mycals=myampcals+mypcals
        myrestcals = mypolcals+myunpolcals+myocals
        myallcals = list(set(myrestcals+mycals))
	logging.info("My cals are %s", str(mycals))
	logging.info("My all cals are %s", str(myallcals))
	i=0
	if os.path.isdir(str(msfilename)+'.AP.G'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.AP.G'+mycalsuffix)
#	for i in range(0,len(mycals)):
#		mygaincal_ap2(msfilename,mycals[i],ref_ant,gainspw,uvracal,mycalsuffix)
##################
        for i in range(0,len(myallcals)):
                mygaincal_ap2(msfilename,myallcals[i],ref_ant,gainspw,uvracal,mycalsuffix)
#################
# Get flux scale
	if os.path.isdir(str(msfilename)+'.fluxscale'+mycalsuffix) == True:
		os.system('rm -rf '+str(msfilename)+'.fluxscale'+mycalsuffix)
########################################
# Modifying here for polcal
	if dopolcal == False:
		if mypcals !=[]:
#			if '3C286' in myampcals:
#				myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(mypcals)),mycalsuffix)
#				myfluxscaleref = '3C286'
#			elif '3C147' in myampcals:
#				myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(mypcals)),mycalsuffix)
#				myfluxscaleref = '3C147'
#			else:
#				myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(mypcals)),mycalsuffix)
#				myfluxscaleref = myampcals[0]
#			logging.info(myfluxscale)
#			mygaintables =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]			
                        if '3C286' in myampcals:
                                myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(mypcals+myrestcals)),mycalsuffix)
                                myfluxscaleref = '3C286'
                        elif '3C147' in myampcals:
                                myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(mypcals+myrestcals)),mycalsuffix)
                                myfluxscaleref = '3C147'
                        else:
                                myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(mypcals+myrestcals)),mycalsuffix)
                                myfluxscaleref = myampcals[0]
                        logging.info(myfluxscale)
                        mygaintables_rest =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
                        mygaintables =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
		else:
			mygaintables =[str(msfilename)+'.AP.G'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
	                if '3C286' in myampcals:
        	                myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(myrestcals)),mycalsuffix)
                	        myfluxscaleref = '3C286'
	                elif '3C147' in myampcals:
        	                myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(myrestcals)),mycalsuffix)
                	        myfluxscaleref = '3C147'
	                else:
        	                myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(myrestcals)),mycalsuffix)
                	        myfluxscaleref = myampcals[0]
	                logging.info(myfluxscale)
        	        mygaintables_rest =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
###############################################################
		for i in range(0,len(myampcals)):
			default(applycal)
			applycal(vis=msfilename, field=myampcals[i], spw = flagspw, gaintable=mygaintables, gainfield=[myampcals[i],'',''], 
        			 interp=['nearest','',''], calwt=[False], parang=False)
                logging.info(mygaintables)
                gainfields = [myampcals[i],kcorrfield,'']
                logging.info(gainfields)
#For phase calibrator:
		if mypcals !=[]:
	                for i in range(0,len(mypcals)):
				default(applycal)
				applycal(vis=msfilename, field=mypcals[i], spw = flagspw, gaintable=mygaintables, gainfield=[mypcals[i],kcorrfield,myampcals], 
				         interp=['nearest','','nearest'], calwt=[False], parang=False)
                        logging.info(mygaintables)
                        gainfields = [mypcals[i],kcorrfield,myampcals]
                        logging.info(gainfields)
###################
#	        if myrestcals !=[]:
#        	        default(applycal)
#                	applycal(vis=msfilename, field=str(', '.join(myrestcals)), spw = flagspw, gaintable=mygaintables_rest, gainfield=str(', '.join(myrestcals)),
#                        	 interp=['nearest','','nearest'], calwt=[False], parang=False)
	        if myrestcals !=[]:
        	        for i in range(0,len(myrestcals)):
                	        default(applycal)
                        	applycal(vis=msfilename, field=myrestcals[i], spw = flagspw, gaintable=mygaintables_rest, gainfield=[myrestcals[i],kcorrfield,myampcals],
                                	 interp=['nearest','','nearest'], calwt=[False], parang=False)
	                        logging.info(mygaintables_rest)
        	                gainfields = [myrestcals[i],kcorrfield,myampcals]
                	        logging.info(gainfields)

###################
#For the target:
		if target ==True:
			if mypcals !=[]:
				default(applycal)
				applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
			        	 gainfield=[str(', '.join(mypcals)),'',''],interp=['linear','','nearest'], calwt=[False], parang=False)
	                        logging.info(mygaintables)
        	                gainfields = [str(', '.join(mypcals)),'','']
                	        logging.info(gainfields)
#			else:
#				default(applycal)
#				applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
#		        	 gainfield=[str(', '.join(myampcals)),'',''],interp=['linear','','nearest'], calwt=[False], parang=False)
#				logging.info(mygaintables)
#       	                gainfields = [str(', '.join(myampcals)),'','']
#                	        logging.info(gainfields)
############new code to use flux cal with max scans as the pcal#####
			else:
				default(applycal)
				applycal(vis=msfilename, field=str(', '.join(mytargets)), spw = flagspw, gaintable=mygaintables,
			        	 gainfield=[pampcal,'',''],interp=['linear','','nearest'], calwt=[False], parang=False)	
                	        logging.info(mygaintables)
				gainfields = [pampcal,'','']
                	        logging.info(gainfields)
#################################################################
		logging.info("Finished re-calibration.")
        	logging.info("A flagging summary is provided for the MS file.")
        	flagsummary(msfilename)
	elif dopolcal == True:
		npols = getpols(msfilename)
		logging.info('You have chosen to do polcal, checking number of pols, npols %s', str(npols)) 
		if npols < 4:
			sys.exit("Not sufficient polarization products in the file. Set dopolcal to False.")
		elif npols == 4:
			logging.info('Found 4 pol products. Proceeding for pol cal.') 
			mychnu = freq_info(msfilename)
			logging.info('Found freqs %s', str(mychnu))
# fix targets
			myfields = getfields(msfilename)
			stdcals = ['3C48','3C147','3C286','0542+498','1331+305','0137+331']
			stdpolcals = ['3C286','3C138']
			stdunpolcals = ['3C84']
			stdothercal = ['OQ208']
			vlacals = np.loadtxt('./vla-cals.list',dtype='string')
			myampcals =[]
			mypcals=[]
			mypolcals =[]
			myunpolcals =[]
			mytargets=[]
			myocals =[]
                        for i in range(0,len(myfields)):
                                if myfields[i] in stdcals:
                                        myampcals.append(myfields[i])
                                elif myfields[i] in vlacals:
                                        mypcals.append(myfields[i])
                                else:
                                        mytargets.append(myfields[i])
                                if myfields[i] in stdpolcals:
                                        mypolcals.append(myfields[i])
                                if myfields[i] in stdunpolcals:
                                        myunpolcals.append(myfields[i])
                                if myfields[i] in stdothercal:
                                        myocals.append(myfields[i])
			mybpcals = myampcals
			if '3C138' in mypolcals:
				mytransfield  = mypcals+myunpolcals+myocals+['3C138']
			else:
				mytransfield =mypcals+myunpolcals+myocals
		        try:
                		mytargets.remove('3C84')
		        except ValueError:
                		logging.info("3C84 is not in the target list.")
		        try:
                		mytargets.remove('OQ208')
		        except ValueError:
                		logging.info("OQ208 is not in the target list.")
			logging.info('Amplitude caibrators are %s', str(myampcals))
			logging.info('Phase calibrators are %s', str(mypcals))
			logging.info('Polarized calibrators are %s', str(mypolcals))
			logging.info('Un-polarized calibrators are %s', str(myunpolcals))
			logging.info('Other calibrator is %s',str(myocals))
			logging.info('Target sources are %s', str(mytargets))
			logging.info('Transfer fields are %s', str(mytransfield))
# need a condition to see if the pcal is same as 
			ampcalscans =[]
			for i in range(0,len(myampcals)):
				ampcalscans.extend(getscans(msfilename, myampcals[i]))
			logging.info('Amp cal scans are %s', str(ampcalscans))
			pcalscans=[]
			for i in range(0,len(mypcals)):
				pcalscans.extend(getscans(msfilename, mypcals[i]))
			logging.info('Phase cal scans are %s', str(pcalscans))
			tgtscans=[]
			for i in range(0,len(mytargets)):
				tgtscans.extend(getscans(msfilename,mytargets[i]))
			logging.info('Target scans are %s', str(tgtscans))
			if '3C286' in myampcals:
				polcalib ='3C286' # hardcoded for now
			else:
				polcalib = myampcals[0]
			logging.info('Pol cal is  %s', str(polcalib))			
#			if '3C84' in mytargets:
#				unpolcalib = '3C84' # hardcoded for now
#			else:
#				unpolcalib =''
			unpolcalib = myunpolcals[0]  # hardcoded for now
			logging.info('Un-pol cal is  %s', str(unpolcalib))
#			if mypcals !=[]:
#				if '3C286' in myampcals:
#					myfluxscale= getfluxcal2(msfilename,'3C286',str(', '.join(mypcals)),mycalsuffix)
#					myfluxscaleref = '3C286'
#				elif '3C147' in myampcals:
#					myfluxscale= getfluxcal2(msfilename,'3C147',str(', '.join(mypcals)),mycalsuffix)
#					myfluxscaleref = '3C147'
#				else:
#					myfluxscale= getfluxcal2(msfilename,myampcals[0],str(', '.join(mypcals)),mycalsuffix)
#					myfluxscaleref = myampcals[0]
#				logging.info(myfluxscale)
#				mygaintables =[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]			
#			else:
##			morecals = myunpolcals+myocals+mycals+mypolcals
##			logging.info("Morecals are %s", str(morecals))
##			if os.path.isdir(str(msfilename)+'.AP.G'+mycalsuffix) == True:
##				os.system('rm -rf '+str(msfilename)+'.AP.G'+mycalsuffix)
##			for i in range(0,len(morecals)):
##				mygaincal_ap2(msfilename,morecals[i],ref_ant,gainspw,uvracal,mycalsuffix)
			mygaintables =[str(msfilename)+'.AP.G'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]			
			if 510E6<mychnu[0]<900E6:
				logging.info("You have band-4 data recorded in full stokes mode. Attempting polarization calibration.")
				if polcalib =='3C138':
					iflux = 13.425
					c0 = 3.69531798 
					c1 = 2.27623964
                                	d0 = -14.62526732*(math.pi/180.0)
					d1 = 2.43086171*(math.pi/180.0)
					specidx = -0.57
					preffreq = '550.049MHz'
				elif polcalib == '3C286':
					iflux = 20.854 # Silpa sent this number
					c0 = 7.88907356 
					c1 = 0.98096296
					d0 = 3.30000000e+01*(math.pi/180.0)
					d1 = -9.50548441e-12*(math.pi/180.0)
					specidx = -0.8
					preffreq = '676.71MHz'
				logging.info("Preffreq %s",str(preffreq))
				palpha = specidx
				logging.info("Spectral index %s",str(palpha))
				ppolindex = [c0,c1]
				logging.info("Ppolindex %s",str(ppolindex))
				ppolangle =[d0,d1]
				logging.info("Ppolangle %s",str(ppolangle))
				polsetjy(msfilename,polcalib,flagspw,iflux,palpha,preffreq,ppolindex,ppolangle)
				# need to provide k table, gaintable and bpasstable and corresponding gfields #str(msfilename)+'.AP.G'+mycalsuffix
				logging.info("########K-cross#######")				
				gtables = mygaintables #[str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.B1'+mycalsuffix]
				logging.info("gaintables used %s", str(gtables))
				#[str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
				gfields =[polcalib,myampcals[0],mybpcals]
				logging.info("The gain fields are %s", str(gfields))
				logging.info("polcalib %s",str(polcalib))
				kcrosstab = kcrossgcal1(msfilename,polcalib,flagspw,ref_ant,gtables,gfields)
				# needs earlier k, g and bp tables+kcross; gfields needs polcalib appended
				# needs unpolcalib = ?  can either be in  unpolcals =['3C84','OQ208'] or in
				# polcals = ['3C286','3C138']
				mygaintables.append(kcrosstab)
				gtables = mygaintables#[str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.B1'+mycalsuffix,kcrosstab]
				logging.info("########leakage######")				
				logging.info(str(gtables))
#				gfields =[str(', '.join(mycals)),myampcals[0],mybpcals,polcalib]#[myampcals[0],mycals,mybpcals,polcalib]
				gfields =[unpolcalib,myampcals[0],mybpcals,polcalib]#[myampcals[0],mycals,mybpcals,polcalib]
				logging.info("gfields used as %s",str(gfields))
				logging.info("unpolcalib %s",str(unpolcalib))
				leakagetab1 = polcalleakage(msfilename,unpolcalib,flagspw,ref_ant,gtables,gfields)
				# update after leakage cal done
				#gaintable = [kcorrfile, bpassfile, gainfile, kcross1, leakage1], 
				#gainfield = [kcorrfield,bpassfield,polcalib,polcalib,unpolcalib])
				mygaintables.append(leakagetab1)
				gtables = mygaintables
				logging.info("########Polangle######")				
				logging.info("gaintables used %s", str(gtables))
				#gtables = mygaintables+kcrosstab+leakagetab1 #[str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.fluxscale'+mycalsuffix,str(msfilename)+'.B1'+mycalsuffix,kcrosstab,leakagetab1]
#				gfields =[str(', '.join(mycals)),myampcals[0],mybpcals,polcalib,unpolcalib]#[myampcals[0],mycals,mybpcals,polcalib,unpolcalib]
				gfields =[polcalib,myampcals[0],mybpcals,polcalib,unpolcalib]#[myampcals[0],mycals,mybpcals,polcalib,unpolcalib]
				logging.info("gainfields used %s",str(gfields))
				logging.info("polcalib %s",str(polcalib))
				polangtab = polcalcross(msfilename, polcalib,ref_ant, gtables, gfields)
				# needs inputs
				logging.info("########Pol Flux scale######")				
				fluxfield = myampcals[0]
				transferfield = str(', '.join(mytransfield))
				logging.info('transfer fields are %s', str(transferfield))
				logging.info('flux fields are %s', str(fluxfield))
				gainfile = str(msfilename)+'.AP.G'+mycalsuffix #str(msfilename)+'.fluxscale'#+mycalsuffix #str(msfilename)+'pfluxscale'+mycalsuffix
				fluxfile = str(msfilename)+'.fluxscale'+mycalsuffix+'pol'
				pfluxscale(msfilename,gainfile,fluxfield,transferfield,fluxfile)
				mygaintables = [fluxfile,str(msfilename)+'.K1'+mycalsuffix, str(msfilename)+'.B1'+mycalsuffix]
				#applycal needs proper ref fields and tables
				logging.info("########Pol applycal flux cal######")				
				gtables = mygaintables.append(polangtab)#[str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.B1'+mycalsuffix,fluxfile, kcrosstab,leakagetab1,polangtab]#[kcorrfile,bpassfile, fluxfile, kcross1, leakage1, polang1]
				logging.info("gaintables used %s", str(gtables))
				gfields =[myampcals[0],myampcals[0],mybpcals,myampcals[0],polcalib,unpolcalib,polcalib]# [myampcals[0],mybpcals,myampcals[0],polcalib,unpolcalib,polcalib] #[kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib]
				logging.info("gainfields used %s",str(gfields))
				logging.info("fluxfield used %s",str(fluxfield))
				papplycal_fcal(msfilename,fluxfield,flagspw,gtables,gfields)
				logging.info("########Pol applycal phase cals######")				
###########################################
				gtables = [str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.B1'+mycalsuffix,fluxfile, kcrosstab,leakagetab1,polangtab]#[kcorrfile,bpassfile, fluxfile, kcross1,
 #leakage1, polang1]
				logging.info("gaintables used %s", str(gtables))
				gfields = [myampcals[0],mybpcals,myampcals[0],polcalib,unpolcalib,polcalib] #[kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib]
				logging.info("gainfields used %s",str(gfields))
				logging.info("phase cals %s",str(', '.join(mypcals)))
				papplycal_pcal(msfilename,str(', '.join(mypcals)),flagspw,gtables,gfields)
				logging.info("########Pol applycal polcalib######")				
###########################################
				gtables = [str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.B1'+mycalsuffix,fluxfile, kcrosstab,leakagetab1,polangtab]#[kcorrfile,bpassfile, fluxfile, kcross1,
 #leakage1, polang1]
				logging.info("gaintables used %s", str(gtables))
				gfields = [myampcals[0],mybpcals,myampcals[0],polcalib,unpolcalib,polcalib] #[kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib]
				logging.info("gainfields used %s",str(gfields))
				logging.info("polcalib %s",str(polcalib))
				papplycal_polcal(msfilename,polcalib,flagspw,gtables,gfields)
###########################################
				logging.info("########Pol applycal un polcalib######")				
				gtables = [str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.B1'+mycalsuffix,fluxfile, kcrosstab,leakagetab1,polangtab]#[kcorrfile,bpassfile, fluxfile, kcross1,
# leakage1, polang1]
				logging.info("gaintables used %s", str(gtables))
				gfields = [myampcals[0],mybpcals,myampcals[0],polcalib,unpolcalib,polcalib] #[kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib]
				logging.info("gainfields used %s",str(gfields))
				logging.info("Unpolcalib %s",str(unpolcalib))
				papplycal_unpolcal(msfilename,unpolcalib,flagspw,gtables,gfields)
###########################################
				logging.info("########Pol applycal targets######")				
				gtables = [str(msfilename)+'.K1'+mycalsuffix,str(msfilename)+'.B1'+mycalsuffix,fluxfile, kcrosstab,leakagetab1,polangtab]#[kcorrfile,bpassfile, fluxfile, kcross1,
# leakage1, polang1]
				logging.info("gaintables used %s", str(gtables))
				if mypcals != []:
					gfields = [myampcals[0],mybpcals,', '.join(mypcals),polcalib,unpolcalib,polcalib] #[kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib]
				else:
#					gfields = [myampcals[0],mybpcals,myampcals[0],polcalib,unpolcalib,polcalib] #[kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib]
					gfields = [myampcals[0],mybpcals,pampcal,polcalib,unpolcalib,polcalib] #[kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib]
				logging.info("gainfields used %s",str(gfields))
				logging.info("targets %s",str(', '.join(mytargets)))
				papplycal_target(msfilename,str(', '.join(mytargets)),flagspw,gtables,gfields)
# Need to make sure that all the tables are received by the functions properly.
###########################################
# Step to plot the results
#############################################################
# SPLIT step
#############################################################
if dosplit == True:
#	assert os.path.isdir(msfilename), "dosplit = True but ms file not found."
        try:
                assert os.path.isdir(msfilename), "dosplit = True but ms file not found."
        except AssertionError:
                logging.info("dosplit = True but ms file not found.")
                sys.exit()
	logging.info("The data on targets will be split into separate files.")
	casalog.filter('INFO')
# fix targets
#	myfields = getfields(msfilename)
#	stdcals = ['3C48','3C147','3C286','0542+498','1331+305','0137+331']
#	vlacals = np.loadtxt('./vla-cals.list',dtype='string')
#	myampcals =[]
#	mypcals=[]
#	mytargets=[]
#	for i in range(0,len(myfields)):
#		if myfields[i] in stdcals:
#			myampcals.append(myfields[i])
#		elif myfields[i] in vlacals:
#			mypcals.append(myfields[i])
#		else:
#			mytargets.append(myfields[i])
#################################### new code below #######
# fix targets
	myfields = getfields(msfilename)
	stdcals = ['3C48','3C147','3C286','0542+498','1331+305','0137+331']
	stdpolcals = ['3C286','3C138']
	stdunpolcals = ['3C84']
	stdothercal = ['OQ208']
	vlacals = np.loadtxt('./vla-cals.list',dtype='string')
	myampcals =[]
	mypcals=[]
	mypolcals =[]
	myunpolcals =[]
	mytargets=[]
	myocals =[]
	for i in range(0,len(myfields)):
	        if myfields[i] in stdcals:
	                myampcals.append(myfields[i])
	        elif myfields[i] in vlacals:
	                mypcals.append(myfields[i])
	        else:
	                mytargets.append(myfields[i])
	        if myfields[i] in stdpolcals:
	                mypolcals.append(myfields[i])
	        if myfields[i] in stdunpolcals:
	                myunpolcals.append(myfields[i])
	        if myfields[i] in stdothercal:
	                myocals.append(myfields[i])
	mybpcals = myampcals
	if '3C138' in mypolcals:
	        mytransfield  = mypcals+myunpolcals+myocals+['3C138']
	else:
	        mytransfield =mypcals+myunpolcals+myocals
	try:
	        mytargets.remove('3C84')
	except ValueError:
	        logging.info("3C84 is not in the target list.")
	try:
	        mytargets.remove('OQ208')
	except ValueError:
	        logging.info("OQ208 is not in the target list.")
	try:
	        mytargets.remove('3C138')
	except ValueError:
	        logging.info("3C138 is not in the target list.")
	logging.info('Amplitude caibrators are %s', str(myampcals))
	logging.info('Phase calibrators are %s', str(mypcals))
	logging.info('Polarized calibrators are %s', str(mypolcals))
	logging.info('Un-polarized calibrators are %s', str(myunpolcals))
	logging.info('Other calibrator is %s',str(myocals))
	logging.info('Target sources are %s', str(mytargets))
	logging.info('Transfer fields are %s', str(mytransfield))
###########################################################
        gainspw1,goodchans,flg_chans,pols = getgainspw(msfilename)
	for i in range(0,len(mytargets)):
		if os.path.isdir(mytargets[i]+'split.ms') == True:
                        logging.info("The existing split file will be deleted.")
			os.system('rm -rf '+mytargets[i]+'split.ms')
                logging.info("Splitting target source data.")
                logging.info(gainspw1)
		splitfilename = mysplitinit(msfilename,mytargets[i],gainspw1,1)
#############################################################
# Flagging on split file
#############################################################

if flagsplitfile == True:
        try:
                assert os.path.isdir(splitfilename), "flagsplitfile = True but the split file not found."
        except AssertionError:
                logging.info("flagsplitfile = True but the split file not found.")
                sys.exit()
        logging.info("Now proceeding to flag on the split file.")
	myantselect =''
	mytfcrop(splitfilename,'',myantselect,8.0,8.0,'DATA','')
	a, b = getbllists(splitfilename)
	tdev = 6.0
	fdev = 6.0
	myrflag(splitfilename,'',a[0],tdev,fdev,'DATA','')
	tdev = 5.0
	fdev = 5.0
	myrflag(splitfilename,'',b[0],tdev,fdev,'DATA','')
	logging.info("A flagging summary is provided for the MS file.")
        flagsummary(splitfilename)
#############################################################
# SPLIT AVERAGE
#############################################################
if dosplitavg == True:
        try:
                assert os.path.isdir(splitfilename), "dosplitavg = True but the split file not found."
        except AssertionError:
                logging.info("dosplitavg = True but the split file not found.")
                sys.exit()
	logging.info("Your data will be averaged in frequency.")
        if os.path.isdir('avg-'+splitfilename) == True:
                os.system('rm -rf avg-'+splitfilename)
                if os.path.isdir('avg-'+splitfilename+'.flagversions') == True:
                       os.system('rm -rf avg-'+splitfilename+'.flagversions')
	splitavgfilename = mysplitavg(splitfilename,'','',chanavg)


if doflagavg == True:
        try:
                assert os.path.isdir(splitavgfilename), "doflagavg = True but the splitavg file not found."
        except AssertionError:
                logging.info("doflagavg = True but the splitavg file not found.")
                sys.exit()
	logging.info("Flagging on freqeuncy averaged data.")
	a, b = getbllists(splitavgfilename)
	myrflagavg(splitavgfilename,'',b[0],6.0,6.0,'DATA','')
	myrflagavg(splitavgfilename,'',a[0],6.0,6.0,'DATA','')
        logging.info("A flagging summary is provided for the MS file.")
        flagsummary(splitavgfilename)

############################################################

if makedirty == True:
        try:
                assert os.path.isdir(splitavgfilename), "makedirty = True but the splitavg file not found."
        except AssertionError:
                logging.info("makedirty = True but the splitavg file not found.")
                sys.exit()
	myfile2 = splitavgfilename
	logging.info("A flagging summary is provided for the MS file.")
	flagsummary(splitavgfilename)
        mytclean(myfile2,0,mJythreshold,0,imcellsize,imsize_pix,use_nterms,nwprojpl)

if doselfcal == True:
	if dosubbandselfcal == True:
		if domidselfcal == True:
			try:
			        assert os.path.isdir(selfcalvis), "domidselfcal = True but the given MS file not found."
			except AssertionError:
			        logging.info("domidselfcal = True but the given MS file not found.")
			        sys.exit()
			myfile2 =[]
			myfile2.append('first-file')
			namepre = selfcalvis.split('selfcal')
                        mygt =[]
                        print(namepre)
                        print(scalsrno)
			for k in range(0,scalsrno):
				myfile2.append(namepre[0]+'selfcal'+str(k)+'.ms')
                                mygt.append('dummy'+str(k))
			myfile2.append(selfcalvis)
                        print(myfile2)
                        nspws = getspws(splitavgfilename)
                        msspw = list(range(0,nspws))
		else:
			try:
			        assert os.path.isdir(splitavgfilename), "dosubbandselfcal = True but the splitavg file not found."
			except AssertionError:
			        logging.info("dosubbandselfcal = True but the splitavg file not found.")
			        sys.exit()
	                nspws = getspws(splitavgfilename)
	                print(nspws)
                        myfile2 = [splitavgfilename]
	                if nspws == 1:
                                bw=getbw(splitavgfilename)
                                if bw<=32E06:
                                        raise Exception("GSB files cannot be subbanded. Make dosubbandselfcal False")                            
	                        mygainspw, msspw = makesubbands(myfile2,subbandchan)
#	        		bw=getbw(splitavgfilename)
#	        		if bw<=32E06:
#	        			raise Exception("GSB files cannot be subbanded. Make dosubbandselfcal False")
	                elif nspws > 1:
	                        msspw = list(range(0,nspws))
	                        print(msspw)
			casalog.filter('INFO')
			logging.info("A flagging summary is provided for the MS file.")
#			flagsummary(splitavgfilename)
#			clearcal(vis = splitavgfilename)
			myfile2 = [splitavgfilename]
                        clearcal(vis=myfile2[0])
                        mygt =[]
		if usetclean == True:
			midsubbandselfcal(myfile2,subbandchan,ref_ant,scaloops,pcaloops,mJythreshold,imcellsize,imsize_pix,use_nterms,nwprojpl,scalsolints,clipresid,'','',False,niter_start,msspw,clean_robust,domidselfcal,scalsrno,mygt)
	else:
		if domidselfcal == True:
			try:
			        assert os.path.isdir(selfcalvis), "domidselfcal = True but the given MS file not found."
			except AssertionError:
			        logging.info("domidselfcal = True but the given MS file not found.")
			        sys.exit()
			myfile2 =[]
			myfile2.append('first-file')
			namepre = selfcalvis.split('selfcal')
                        mygt =[]
			for k in range(0,scalsrno):
				myfile2.append(namepre[0]+'selfcal'+str(k)+'.ms')
                                mygt.append('dummy'+str(k))
			myfile2.append(selfcalvis)
		else:
			try:
			        assert os.path.isdir(splitavgfilename), "doselfcal = True but the splitavg file not found."
			except AssertionError:
			        logging.info("doselfcal = True but the splitavg file not found.")
			        sys.exit()
			casalog.filter('INFO')
			logging.info("A flagging summary is provided for the MS file.")
			flagsummary(splitavgfilename)
			clearcal(vis = splitavgfilename)
			myfile2 = [splitavgfilename]
		if usetclean == True:
			myselfcal(myfile2,ref_ant,scaloops,pcaloops,mJythreshold,imcellsize,imsize_pix,use_nterms,nwprojpl,scalsolints,clipresid,'','',False,niter_start,clean_robust,domidselfcal,scalsrno)
			



if dopolimages_target == True:
        tcleanQ(finalvis,imcellsize,imsize_pix, use_nterms,nwprojpl,clean_robust)
        tcleanU(finalvis,imcellsize,imsize_pix, use_nterms,nwprojpl,clean_robust)
        tcleanV(finalvis,imcellsize,imsize_pix, use_nterms,nwprojpl,clean_robust)




if dosplit_calibrator == True:
# fix targets
        myfields = getfields(msfilename)
        stdcals = ['3C48','3C147','3C286','0542+498','1331+305','0137+331']
        stdpolcals = ['3C286','3C138']
        stdunpolcals = ['3C84']
        stdothercal = ['OQ208']
        vlacals = np.loadtxt('./vla-cals.list',dtype='string')
        myampcals =[]
        mypcals=[]
        mypolcals =[]
        myunpolcals =[]
        mytargets=[]
        myocals =[]
        for i in range(0,len(myfields)):
                if myfields[i] in stdcals:
                        myampcals.append(myfields[i])
                elif myfields[i] in vlacals:
                        mypcals.append(myfields[i])
                else:
                        mytargets.append(myfields[i])
                if myfields[i] in stdpolcals:
                        mypolcals.append(myfields[i])
                if myfields[i] in stdunpolcals:
                        myunpolcals.append(myfields[i])
                if myfields[i] in stdothercal:
                        myocals.append(myfields[i])
        mybpcals = myampcals
        if '3C138' in mypolcals:
                mytransfield  = mypcals+myunpolcals+myocals+['3C138']
        else:
                mytransfield =mypcals+myunpolcals+myocals
        try:
                mytargets.remove('3C84')
        except ValueError:
                logging.info("3C84 is not in the target list.")
        try:
                mytargets.remove('OQ208')
        except ValueError:
                logging.info("OQ208 is not in the target list.")
        try:
                mytargets.remove('3C138')
        except ValueError:
                logging.info("3C138 is not in the target list.")
        logging.info('Amplitude caibrators are %s', str(myampcals))
        logging.info('Phase calibrators are %s', str(mypcals))
        logging.info('Polarized calibrators are %s', str(mypolcals))
        logging.info('Un-polarized calibrators are %s', str(myunpolcals))
        logging.info('Other calibrator is %s',str(myocals))
        logging.info('Target sources are %s', str(mytargets))
        logging.info('Transfer fields are %s', str(mytransfield))
        myallcals = myampcals+mypcals+mypolcals+myunpolcals+myocals
        uniq_cals = list(set(myallcals))
        logging.info('The calibrators in this file are %s', str(uniq_cals))
        gainspw1,goodchans,flg_chans,pols = getgainspw(msfilename)
        for i in range(0,len(uniq_cals)):
                if os.path.isdir(uniq_cals[i]+'split.ms') == True:
                        logging.info("The existing split file will be deleted.")
                        os.system('rm -rf '+uniq_cals[i]+'split.ms')
                if os.path.isdir(uniq_cals[i]+'split.ms.flagversions') == True:
                        os.system('rm -rf '+uniq_cals[i]+'split.ms.flagversions')
                logging.info("Splitting calibrator source data.")
                logging.info(gainspw1)
                splitfilename = mysplitinit(msfilename,uniq_cals[i],gainspw1,1)
# Flagging on split file
#############################################################
#if flagsplitfile == True:
                try:
                        assert os.path.isdir(splitfilename), "the split file of the claibrator not found."
                except AssertionError:
                        logging.info("The split file not found.")
                        sys.exit()
                logging.info("Now proceeding to flag on the split file.")
                myantselect =''
                mytfcrop(splitfilename,'',myantselect,4.0,4.0,'DATA','')
#               a, b = getbllists(splitfilename)
#               tdev = 6.0
#               fdev = 6.0
                myrflag(splitfilename,'',myantselect,4.0,4.0,'DATA','')
#               tdev = 5.0
#               fdev = 5.0
#               myrflag(splitfilename,'',b[0],tdev,fdev,'DATA','')
                logging.info("A flagging summary is provided for the MS file.")
                flagsummary(splitfilename)
#############################################################
# SPLIT AVERAGE
#############################################################
#if dosplitavg == True:
                try:
                        assert os.path.isdir(splitfilename), "The split file for the calibrator not found."
                except AssertionError:
                        logging.info("The split file not found.")
                        sys.exit()
                logging.info("Your data will be averaged in frequency.")
                if os.path.isdir('avg-'+splitfilename) == True:
                        os.system('rm -rf avg-'+splitfilename)
                if os.path.isdir('avg-'+splitfilename+'.flagversions') == True:
                        os.system('rm -rf avg-'+splitfilename+'.flagversions')
                splitavgfilename = mysplitavg(splitfilename,'','',chanavg)
#if doflagavg == True:
                try:
                        assert os.path.isdir(splitavgfilename), "The splitavg file not found."
                except AssertionError:
                        logging.info("The splitavg file not found.")
                        sys.exit()
                logging.info("Flagging on freqeuncy averaged data.")
#               a, b = getbllists(splitavgfilename)
                myrflagavg(splitavgfilename,'','',4.0,4.0,'DATA','')
                myrflagavg(splitavgfilename,'','',4.0,4.0,'DATA','')
                logging.info("A flagging summary is provided for the MS file.")
                flagsummary(splitavgfilename)
#               tcleanQ(splitavgfilename,cell,3000, mynterms1,mywproj,clean_robust)
#               tcleanU(splitavgfilename,cell,3000, mynterms1,mywproj,clean_robust)
#               tcleanV(splitavgfilename,cell,3000, mynterms1,mywproj,clean_robust)



############

