



# FUNCTIONS
###############################################################
# A library of function that are used in the pipeline

def vislistobs(msfile):
	'''Writes the verbose output of the task listobs.'''
	ms.open(msfile)  
	outr=ms.summary(verbose=True,listfile=msfile+'.list')
#	print("A file containing listobs output is saved.")
	try:
		assert os.path.isfile(msfile+'.list'),logging.info("A file containing listobs output is saved.")
	except AssertionError:
		logging.info("The listobs output as not saved in a .list file. Please check the CASA log.")
	return outr

def getpols(msfile):
        '''Get the number of polarizations in the file'''
        msmd.open(msfile)
        polid = msmd.ncorrforpol(0)
        msmd.done()
        return polid

def mypols(inpvis,mypolid):
    msmd.open(inpvis)
    # get correlation types for polarization ID 3
    corrtypes = msmd.corrprodsforpol(0)
    msmd.done()
    return corrtypes

def getfields(msfile):
	'''get list of field names in the ms'''
	msmd.open(msfile)  
	fieldnames = msmd.fieldnames()
	msmd.done()
	return fieldnames

def getscans(msfile, mysrc):
	'''get a list of scan numbers for the specified source'''
	msmd.open(msfile)
	myscan_numbers = msmd.scansforfield(mysrc)
	myscanlist = myscan_numbers.tolist()
	msmd.done()
	return myscanlist

def getantlist(myvis,scanno):
	msmd.open(myvis)
	antenna_name = msmd.antennasforscan(scanno)
	antlist=[]
	for i in range(0,len(antenna_name)):
		antlist.append(msmd.antennanames(antenna_name[i])[0])
	return antlist


def getnchan(msfile):
	msmd.open(msfile)
	nchan = msmd.nchan(0)
	msmd.done()
	return nchan

def getbw(msfile):
	msmd.open(msfile)
	bw = msmd.bandwidths(0)
	msmd.done()
	return bw


def freq_info(ms_file):									
	sw = 0
	msmd.open(ms_file)
	freq=msmd.chanfreqs(sw)								
	msmd.done()
	return freq									

def makebl(ant1,ant2):
	mybl = ant1+'&'+ant2
	return mybl


def getbllists(myfile):
	myfields = getfields(myfile)
	myallscans =[]
	for i in range(0,len(myfields)):
		myallscans.extend(getscans(myfile, myfields[i]))
	myantlist = getantlist(myfile,int(myallscans[0]))
	allbl=[]
	for i in range(0,len(myantlist)):
		for j in range(0,len(myantlist)):
			if j>i:
				allbl.append(makebl(myantlist[i],myantlist[j]))
	mycc=[]
	mycaa=[]
	for i in range(0,len(allbl)):
		if allbl[i].count('C')==2:
			mycc.append(allbl[i])
		else:
			mycaa.append(allbl[i])
	myshortbl =[]
	myshortbl.append(str('; '.join(mycc)))
	mylongbl =[]
	mylongbl.append(str('; '.join(mycaa)))
	return myshortbl, mylongbl


def getbandcut(inpmsfile):
	cutoffs = {'L':0.2, 'P':0.3, '235':0.5, '610':0.2, 'b4':0.2, 'b2':0.7, '150':0.7}
	frange = freq_info(inpmsfile)
	fmin = min(frange)
	fmax = max(frange)
	if fmin > 1000E06:
		fband = 'L'
	elif fmin >500E06 and fmin < 1000E06:
		fband = 'b4'
	elif fmin >260E06 and fmin < 560E06:
		fband = 'P'
	elif fmin > 210E06 and fmin < 260E06:
		fband = '235'
	elif fmin > 80E6 and fmin < 200E6:
		fband = 'b2'
	else:
		"Frequency band does not match any of the GMRT bands."
	logging.info("The frequency band in the file is ")
	logging.info(fband)
	xcut = cutoffs.get(fband)
	logging.info("The mean cutoff used for flagging bad antennas is ")
	logging.info(xcut)
	return xcut

def myvisstatampraw1(myfile,myfield,myspw,myant,mycorr,myscan):
	default(visstat)
	mystat = visstat(vis=myfile,axis="amp",datacolumn="data",useflags=False,spw=myspw,
		field=myfield,selectdata=True,antenna=myant,uvrange="",timerange="",
		correlation=mycorr,scan=myscan,array="",observation="",timeaverage=False,
		timebin="0s",timespan="",maxuvwdistance=0.0,disableparallel=None,ddistart=None,
		taql=None,monolithic_processing=None,intent="",reportingaxes="ddid")
	mymean1 = mystat['DATA_DESC_ID=0']['mean']
	return mymean1

def myvisstatampraw(myfile,myspw,myant,mycorr,myscan):
	default(visstat)
	mystat = visstat(vis=myfile,axis="amp",datacolumn="data",useflags=False,spw=myspw,
		selectdata=True,antenna=myant,uvrange="",timerange="",
		correlation=mycorr,scan=myscan,array="",observation="",timeaverage=False,
		timebin="0s",timespan="",maxuvwdistance=0.0,disableparallel=None,ddistart=None,
		taql=None,monolithic_processing=None,intent="",reportingaxes="ddid")
	mymean1 = mystat['DATA_DESC_ID=0']['mean']
	return mymean1


def mygaincal_ap1(myfile,mycal,myref,myflagspw,myuvracal,calsuffix):
	default(gaincal)
	gaincal(vis=myfile, caltable=str(myfile)+'.AP.G', spw =myflagspw,uvrange=myuvracal,append=True,
		field=mycal,solint = '120s',refant = myref, minsnr = 2.0, solmode ='L1R', gaintype = 'G', calmode = 'ap',refantmode ='strict',
		gaintable = [str(myfile)+'.K1', str(myfile)+'.B1' ], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
		parang = True )
	return gaintable


def mygaincal_ap2(myfile,mycal,myref,myflagspw,myuvracal,calsuffix):
	default(gaincal)
	gaincal(vis=myfile, caltable=str(myfile)+'.AP.G'+calsuffix, spw =myflagspw,uvrange=myuvracal,append=True,
		field=mycal,solint = '120s',refant = myref, minsnr = 2.0, solmode ='L1R', gaintype = 'G', calmode = 'ap',refantmode ='strict',
		gaintable = [str(myfile)+'.K1'+calsuffix, str(myfile)+'.B1'+calsuffix ], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
		parang = True )
	return gaintable

def getfluxcal(myfile,mycalref,myscal):
	myscale = fluxscale(vis=myfile, caltable=str(myfile)+'.AP.G', fluxtable=str(myfile)+'.fluxscale', reference=mycalref, transfer=myscal,
                    incremental=False)
	return myscale


def getfluxcal2(myfile,mycalref,myscal,calsuffix):
	myscale = fluxscale(vis=myfile, caltable=str(myfile)+'.AP.G'+calsuffix, fluxtable=str(myfile)+'.fluxscale'+calsuffix, reference=mycalref,
       	            transfer=myscal, incremental=True)
	return myscale



def mygaincal_ap_redo(myfile,mycal,myref,myflagspw,myuvracal):
	default(gaincal)
	gaincal(vis=myfile, caltable=str(myfile)+'.AP.G.'+'recal', append=True, spw =myflagspw, uvrange=myuvracal,
		field=mycal,solint = '120s',refant = myref, minsnr = 2.0,solmode ='L1R', gaintype = 'G', calmode = 'ap',
		gaintable = [str(myfile)+'.K1'+'recal', str(myfile)+'.B1'+'recal' ], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
		parang = True )
	return gaintable

def getfluxcal_redo(myfile,mycalref,myscal):
	myscale = fluxscale(vis=myfile, caltable=str(myfile)+'.AP.G'+'recal', fluxtable=str(myfile)+'.fluxscale'+'recal', reference=mycalref,
                    transfer=myscal, incremental=False)
	return myscale

def mytfcrop(myfile,myfield,myants,tcut,fcut,mydatcol,myflagspw):
	default(flagdata)
	flagdata(vis=myfile, antenna = myants, field = myfield,	spw = myflagspw, mode='tfcrop', ntime='300s', combinescans=False,
		datacolumn=mydatcol, timecutoff=tcut, freqcutoff=fcut, timefit='line', freqfit='line', flagdimension='freqtime',
		usewindowstats='sum', extendflags = False, action='apply', display='none')
	return


def myrflag(myfile,myfield, myants, mytimdev, myfdev,mydatcol,myflagspw):
	default(flagdata)
	flagdata(vis=myfile, field = myfield, spw = myflagspw, antenna = myants, mode='rflag', ntime='scan', combinescans=False,
		datacolumn=mydatcol, winsize=3, timedevscale=mytimdev, freqdevscale=myfdev, spectralmax=1000000.0, spectralmin=0.0,
		extendflags=False, channelavg=False, timeavg=False, action='apply', display='none')
	return


def myrflagavg(myfile,myfield, myants, mytimdev, myfdev,mydatcol,myflagspw):
	default(flagdata)
	flagdata(vis=myfile, field = myfield, spw = myflagspw, antenna = myants, mode='rflag', ntime='300s', combinescans=True,
		datacolumn=mydatcol, winsize=3,	minchanfrac= 0.8, flagneartime = True, basecnt = True, fieldcnt = True,
		timedevscale=mytimdev, freqdevscale=myfdev, spectralmax=1000000.0, spectralmin=0.0, extendflags=False,
		channelavg=False, timeavg=False, action='apply', display='none')
	return

def getgainspw(msfilename):
        mynchan = getnchan(msfilename)
        logging.info('The number of channels in your file %d',mynchan)
        gmrt235 = False
        gmrt610 = False
        gmrtfreq = 0.0
# check if single pol data
        mypol = getpols(msfilename)
#        logging.info('Your file contains %s polarization products.', mypol)
        if mypol == 1:
#                print("This dataset contains only single polarization data.")
                logging.info('This dataset contains only single polarization data.')
                mychnu = freq_info(msfilename)
                if 200E6< mychnu[0]<300E6:
                        poldata = 'LL'
                        gmrt235 = True
                        gmrt610 = False
                        mynchan = getnchan(msfilename)
                        if mynchan !=256:
#                                print("You have data in the 235 MHz band of dual frequency mode of the GMRT. Currently files only with 256 channels are supported in this pipeline.")
                                logging.info('You have data in the 235 MHz band of dual frequency mode of the GMRT. Currently files only with 256 channels are supported in this pipeline.')
                                sys.exit()
                elif 590E6<mychnu[0]<700E6:
                        poldata = 'RR'
                        gmrt235 = False
                        gmrt610 = True
                        mynchan = getnchan(msfilename)
                        if mynchan != 256:
#                                print("You have data in the 610 MHz band of the dual frequency mode of the legacy GMRT. Currently files only with 256 channels are supported in this pipeline.")
                                logging.info('You have data in the 610 MHz band of the dual frequency mode of the legacy GMRT. Currently files only with 256 channels are supported in this pipeline.')
                                sys.exit()
                else:
                        gmrtfreq = mychnu[0]
#                        print("You have data in a single polarization - most likely GMRT hardware correlator. This pipeline currently does not support reduction of single pol HW correlator data.")
                        logging.info('You have data in a single polarization - most likely GMRT hardware correlator. This pipeline currently does not support reduction of single pol HW correlator data.')
#                        print("The number of channels in this file are %d" %  mychnu[0])
                        logging.info('The number of channels in this file are %d', mychnu[0])
                        sys.exit()
# Now get the channel range.        
        if mynchan == 1024:
                mygoodchans = '0:250~300'   # used for visstat
                flagspw = '0:51~950'
                gainspw = '0:101~900'
                gainspw2 = ''   # central good channels after split file for self-cal
                logging.info("The following channel range will be used.")
        elif mynchan == 2048:
                mygoodchans = '0:1100~1200'   # used for visstat  # edited for pol cal of 32_108
                flagspw = '0:801~1700'
                gainspw = '0:801~1700'
                gainspw2 = ''   # central good channels after split file for self-cal
                logging.info("The following channel range will be used.")
        elif mynchan == 4096:
                mygoodchans = '0:1000~1200'
                flagspw = '0:41~4050'
                gainspw = '0:201~3600'
                gainspw2 = ''   # central good channels after split file for self-cal
                logging.info("The following channel range will be used.")
        elif mynchan == 8192:
                mygoodchans = '0:2000~3000'
                flagspw = '0:500~7800'
                gainspw = '0:1000~7000'
                gainspw2 = ''   # central good channels after split file for self-cal
                logging.info("The following channel range will be used.")
        elif mynchan == 16384:
                mygoodchans = '0:4000~6000'
                flagspw = '0:1000~14500'
                gainspw = '0:2000~13500'
                gainspw2 = ''   # central good channels after split file for self-cal
                logging.info("The following channel range will be used.")
        elif mynchan == 128:
                mygoodchans = '0:50~70'
                flagspw = '0:5~115'
                gainspw = '0:11~115'
                gainspw2 = ''   # central good channels after split file for self-cal
                logging.info("The following channel range will be used.")
        elif mynchan == 256:
#               if poldata == 'LL':
                if gmrt235 == True:
                        mygoodchans = '0:150~160'
                        flagspw = '0:70~220'
                        gainspw = '0:91~190'
                        gainspw2 = ''   # central good channels after split file for self-cal
                        logging.info("The following channel range will be used.")
                elif gmrt610 == True:
                        mygoodchans = '0:100~120'
                        flagspw = '0:11~240'
                        gainspw = '0:21~230'
                        gainspw2 = ''   # central good channels after split file for self-cal   
                        logging.info("The following channel range will be used.")
                else:
                        mygoodchans = '0:150~160'
                        flagspw = '0:11~240'
                        gainspw = '0:21~230'
                        gainspw2 = ''   # central good channels after split file for self-cal
                        logging.info("The following channel range will be used.")
        elif mynchan == 512:
                mygoodchans = '0:200~240'
                flagspw = '0:21~500'
                gainspw = '0:41~490'
                gainspw2 = ''   # central good channels after split file for self-cal   
                logging.info("The following channel range will be used.")
        return gainspw, mygoodchans, flagspw, mypol



def mysplitinit(myfile,myfield,myspw,mywidth):
	'''function to split corrected data for any field'''
	default(mstransform)
	mstransform(vis=myfile, field=myfield, spw=myspw, chanaverage=False, chanbin=mywidth, datacolumn='corrected', outputvis=str(myfield)+'split.ms')
	myoutvis=str(myfield)+'split.ms'
	return myoutvis


def mysplitavg(myfile,myfield,myspw,mywidth):
	'''function to split corrected data for any field'''
#	myoutname=myfile.split('s')[0]+'avg-split.ms'
	myoutname='avg-'+myfile
        default(mstransform)
	mstransform(vis=myfile, field=myfield, spw=myspw, chanaverage=True, chanbin=mywidth, datacolumn='data', outputvis=myoutname)
	return myoutname


def mytclean(myfile,myniter,mythresh,srno,cell,imsize, mynterms1,mywproj,clean_robust):    # you may change the multi-scale inputs as per your field
	nameprefix = getfields(myfile)[0]#myfile.split('.')[0]
	print("The image files have the following prefix =",nameprefix)
	if myniter==0:
		myoutimg = nameprefix+'-dirty-img'
	else:
		myoutimg = nameprefix+'-selfcal'+'img'+str(srno)
	default(tclean)
	if mynterms1 > 1:
		tclean(vis=myfile,
       			imagename=myoutimg, selectdata= True, field='0', spw='', imsize=imsize, cell=cell, robust=clean_robust, weighting='briggs', 
       			specmode='mfs',	nterms=mynterms1, niter=myniter, usemask='auto-multithresh',minbeamfrac=0.1, sidelobethreshold = 2.0,
#			minpsffraction=0.05,
#			maxpsffraction=0.8,
			smallscalebias=0.6, threshold= mythresh, aterm =True, pblimit=-1,
	        	deconvolver='mtmfs', gridder='wproject', wprojplanes=mywproj, scales=[0,5,15],wbawp=False,
			restoration = True, savemodel='modelcolumn', cyclefactor = 0.5, parallel=False,
       			interactive=False)
	else:
		tclean(vis=myfile,
       			imagename=myoutimg, selectdata= True, field='0', spw='', imsize=imsize, cell=cell, robust=clean_robust, weighting='briggs', 
       			specmode='mfs',	nterms=mynterms1, niter=myniter, usemask='auto-multithresh',minbeamfrac=0.1,sidelobethreshold = 2.0,
#			minpsffraction=0.05,
#			maxpsffraction=0.8,
			smallscalebias=0.6, threshold= mythresh, aterm =True, pblimit=-1,
	        	deconvolver='multiscale', gridder='wproject', wprojplanes=mywproj, scales=[0,5,15],wbawp=False,
			restoration = True, savemodel='modelcolumn', cyclefactor = 0.5, parallel=False,
       			interactive=False)
	return myoutimg

def mysbtclean(myfile,myniter,mythresh,srno,cell,imsize, mynterms1,mywproj,clean_robust):    # you may change the multi-scale inputs as per your field
	nameprefix = getfields(myfile)[0]#myfile.split('.')[0]
	print("The image files have the following prefix =",nameprefix)
	if myniter==0:
		myoutimg = nameprefix+'-dirty-img'
	else:
		myoutimg = nameprefix+'-selfcal'+'img'+str(srno)
        try:
                assert os.path.isdir(myoutimg+'.image.tt0'), "The image file exists, imaging will not proceed."
        except AssertionError:
                logging.info("The image file does not exist, thus running tclean.")
        	default(tclean)
	        if mynterms1 > 1:
		        tclean(vis=myfile,
       		            imagename=myoutimg, selectdata= True, field='0', spw='', imsize=imsize, cell=cell, robust=clean_robust, weighting='briggs', 
       			    specmode='mfs',	nterms=mynterms1, niter=myniter, usemask='auto-multithresh',minbeamfrac=0.1, sidelobethreshold = 2.0,
#			minpsffraction=0.05,
#			maxpsffraction=0.8,
			    smallscalebias=0.6, threshold= mythresh, aterm =True, pblimit=-1,
	        	    deconvolver='mtmfs', gridder='wproject', wprojplanes=mywproj, scales=[0,5,15],wbawp=False,
			    restoration = True, savemodel='modelcolumn', cyclefactor = 0.5, parallel=False,
       			    interactive=False)
        	else:
	        	tclean(vis=myfile,
       			imagename=myoutimg, selectdata= True, field='0', spw='', imsize=imsize, cell=cell, robust=clean_robust, weighting='briggs', 
       			specmode='mfs',	nterms=mynterms1, niter=myniter, usemask='auto-multithresh',minbeamfrac=0.1,sidelobethreshold = 2.0,
#			minpsffraction=0.05,
#			maxpsffraction=0.8,
			smallscalebias=0.6, threshold= mythresh, aterm =True, pblimit=-1,
	        	deconvolver='multiscale', gridder='wproject', wprojplanes=mywproj, scales=[0,5,15],wbawp=False,
			restoration = True, savemodel='modelcolumn', cyclefactor = 0.5, parallel=False,
       			interactive=False)
	return myoutimg


def myonlyclean(myfile,myniter,mythresh,srno,cell,imsize,mynterms1,mywproj,clean_robust):
	default(clean)
	clean(vis=myfile,
	selectdata=True,
	spw='',
	imagename='selfcal'+'img'+str(srno),
	imsize=imsize,
	cell=cell,
	mode='mfs',
	reffreq='',
	weighting='briggs',
        robust = clean_robust,
	niter=myniter,
	threshold=mythresh,
	nterms=mynterms1,
	gridmode='widefield',
	wprojplanes=mywproj,
	interactive=False,
	usescratch=True)
	myname = 'selfcal'+'img'+str(srno)
	return myname


def mysplit(myfile,srno):
	filname_pre = getfields(myfile)[0]
	default(mstransform)
	mstransform(vis=myfile, field='0', spw='', datacolumn='corrected', outputvis=filname_pre+'-selfcal'+str(srno)+'.ms')
	myoutvis=filname_pre+'-selfcal'+str(srno)+'.ms'
	return myoutvis

def mysbsplit(myfile,srno):
	filname_pre = getfields(myfile)[0]
	default(mstransform)
	mstransform(vis=myfile, field='0', spw='', datacolumn='corrected', outputvis=filname_pre+'-selfcal'+str(srno)+'.ms')
	myoutvis=filname_pre+'-selfcal'+str(srno)+'.ms'
	return myoutvis


def mygaincal_ap(myfile,myref,srno,pap,mysolint,myuvrascal,mygainspw):
	fprefix = getfields(myfile)[0]
	if pap=='ap':
		mycalmode='ap'
		mysol= mysolint[srno] 
		mysolnorm = True
	else:
		mycalmode='p'
		mysol= mysolint[srno] 
		mysolnorm = False
	if os.path.isdir(fprefix+str(pap)+str(srno)+'.GT'):
		os.system('rm -rf '+fprefix+str(pap)+str(srno)+'.GT')
	default(gaincal)
	gaincal(vis=myfile, caltable=fprefix+str(pap)+str(srno)+'.GT', append=False, field='0', spw=mygainspw,
		uvrange=myuvrascal, solint = mysol, refant = myref, minsnr = 2.0,solmode='L1R', gaintype = 'G',refantmode ='strict',
		solnorm= mysolnorm, calmode = mycalmode, gaintable = [], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
		parang = True )
	mycal = fprefix+str(pap)+str(srno)+'.GT'
	return mycal

def mysbgaincal_ap(myfile,xgt,myref,mygtable,srno,pap,mysolint,myuvrascal,mygainspw):

	fprefix = getfields(myfile)[0]
	if pap=='ap':
		mycalmode='ap'
		mysol= mysolint[srno] 
		mysolnorm = True
	else:
		mycalmode='p'
		mysol= mysolint[srno] 
		mysolnorm = False
	if os.path.isdir(fprefix+str(pap)+str(srno)+str('sb')+str(xgt)+'.GT'):
		os.system('rm -rf '+str(pap)+str(srno)+str('sb')+str(xgt)+'.GT')
	default(gaincal)
	gaincal(vis=myfile, caltable=fprefix+str(pap)+str(srno)+str('sb')+str(xgt)+'.GT', append=False, field='0', spw=str(xgt),
		uvrange=myuvrascal, solint = mysol, refant = myref, minsnr = 2.0,solmode='L1R', gaintype = 'G',
		solnorm= mysolnorm, calmode = mycalmode, gaintable = [], interp = ['nearest,nearestflag', 'nearest,nearestflag' ], 
		parang = True )
		
	mycal = fprefix+str(pap)+str(srno)+str('sb')+str(xgt)+'.GT'
	return mycal



def myapplycal(myfile,mygaintables):
	default(applycal)
	applycal(vis=myfile, field='0', gaintable=mygaintables, gainfield=['0'], applymode='calflag', 
	         interp=['linear'], calwt=False, parang=False)
	print('Ran applycal.')


def mysbapplycal(myfile,mygaintables,xgt):
	default(applycal)
	applycal(vis=myfile, field='0',spw=str(xgt), gaintable=mygaintables, gainfield=['0'], applymode='calflag', 
	         interp=['linear'], calwt=False, parang=False)
	print('Ran applycal.')
	


def flagresidual(myfile,myclipresid,myflagspw):
	default(flagdata)
	flagdata(vis=myfile, mode ='rflag', datacolumn="RESIDUAL_DATA", field='', timecutoff=6.0,  freqcutoff=6.0,
		timefit="line", freqfit="line",	flagdimension="freqtime", extendflags=False, timedevscale=6.0,
		freqdevscale=6.0, spectralmax=500.0, extendpols=False, growaround=False, flagneartime=False,
		flagnearfreq=False, action="apply", flagbackup=True, overwrite=True, writeflags=True)
	default(flagdata)
	flagdata(vis=myfile, mode ='clip', datacolumn="RESIDUAL_DATA", clipminmax=myclipresid,
		clipoutside=True, clipzeros=True, field='', spw=myflagspw, extendflags=False,
		extendpols=False, growaround=False, flagneartime=False,	flagnearfreq=False,
		action="apply",	flagbackup=True, overwrite=True, writeflags=True)
	flagdata(vis=myfile,mode="summary",datacolumn="RESIDUAL_DATA", extendflags=False, 
		name=myfile+'temp.summary', action="apply", flagbackup=True,overwrite=True, writeflags=True)
#


	 

def myselfcal(myfile,myref,nloops,nploops,myvalinit,mycellsize,myimagesize,mynterms2,mywproj1,mysolint1,myclipresid,myflagspw,mygainspw2,mymakedirty,niterstart,clean_robust):
	myref = myref
	nscal = nloops # number of selfcal loops
	npal = nploops # number of phasecal loops
	# selfcal loop
	myimages=[]
	mygt=[]
	myniterstart = niterstart
	myniterend = 200000	
	if nscal == 0:
		i = nscal
		myniter = 0 # this is to make a dirty image
		mythresh = str(myvalinit/(i+1))+'mJy'
		print("Using "+ myfile[i]+" for making only an image.")
		if usetclean == False:
			myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # clean
		else:
			myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # tclean
                if mynterms2 > 1:
                        exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
                else:
                        exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')

	else:
		for i in range(0,nscal+1): # plan 4 P and 4AP iterations
			if mymakedirty == True:
				if i == 0:
					myniter = 0 # this is to make a dirty image
					mythresh = str(myvalinit/(i+1))+'mJy'
					print("Using "+ myfile[i]+" for making only a dirty image.")
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # clean
					else:
						myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')

			else:
				myniter=int(myniterstart*2**i) #myniterstart*(2**i)  # niter is doubled with every iteration int(startniter*2**count)
				if myniter > myniterend:
					myniter = myniterend
				mythresh = str(myvalinit/(i+1))+'mJy'
				if i < npal:
					mypap = 'p'
#					print("Using "+ myfile[i]+" for imaging.")
					try:
						assert os.path.isdir(myfile[i])
					except AssertionError:
						logging.info("The MS file not found for imaging.")
						sys.exit()
					logging.info("Using "+ myfile[i]+" for imaging.")
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # clean
					else:
						myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')
					myimages.append(myimg)	# list of all the images created so far
					flagresidual(myfile[i],clipresid,'')
					if i>0:
						#myctables = mygaincal_ap(myfile[i],myref,mygt[i-1],i,mypap,mysolint1,uvrascal,mygainspw2)
						myctables = mygaincal_ap(myfile[i],myref,i,mypap,mysolint1,uvrascal,mygainspw2)
					else:					
						#myctables = mygaincal_ap(myfile[i],myref,mygt,i,mypap,mysolint1,uvrascal,mygainspw2)						
						myctables = mygaincal_ap(myfile[i],myref,i,mypap,mysolint1,uvrascal,mygainspw2)						
					mygt.append(myctables) # full list of gaintables
					if i < nscal+1:
						myapplycal(myfile[i],mygt[i])
						myoutfile= mysplit(myfile[i],i)
						myfile.append(myoutfile)
				else:
					mypap = 'ap'
#					print("Using "+ myfile[i]+" for imaging.")
                                        try:
                                                assert os.path.isdir(myfile[i])
                                        except AssertionError:
                                                logging.info("The MS file not found for imaging.")
                                                sys.exit()
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # clean
					else:
						myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')
					myimages.append(myimg)	# list of all the images created so far
					flagresidual(myfile[i],clipresid,'')
					if i!= nscal:
						#myctables = mygaincal_ap(myfile[i],myref,mygt[i-1],i,mypap,mysolint1,'',mygainspw2)
						myctables = mygaincal_ap(myfile[i],myref,i,mypap,mysolint1,'',mygainspw2)
						mygt.append(myctables) # full list of gaintables
						if i < nscal+1:
							myapplycal(myfile[i],mygt[i])
							myoutfile= mysplit(myfile[i],i)
							myfile.append(myoutfile)
#				print("Visibilities from the previous selfcal will be deleted.")
				logging.info("Visibilities from the previous selfcal will be deleted.")
				if i < nscal:
					fprefix = getfields(myfile[i])[0]
					myoldvis = fprefix+'-selfcal'+str(i-1)+'.ms'
#					print("Deleting "+str(myoldvis))
					logging.info("Deleting "+str(myoldvis))
					os.system('rm -rf '+str(myoldvis))
#			print('Ran the selfcal loop')
	return myfile, mygt, myimages

#def getspws(myfile):
#        ms.open(myfile)
#        metadata = ms.metadata()
#        ms.done()
#        nspw = metadata.nspw()
#        metadata.done()
#        return nspw

def getspws(myfile):
        nspws = vishead(vis =myfile,mode ='list')
        nspw = len(nspws['freq_group_name'][0])
        return nspw

def makesubbands(myfile,subbandchan):
        os.system('rm -r msimg*')
        splitspw=[]
        msspw=[]
        gainsplitspw=[]
        xchan=subbandchan
        myx=getnchan(myfile[0])
        if myx>xchan:
                mynchani=myx
                xs=0
                while mynchani>0:
                        if mynchani>xchan:
                            spwi='0:'+str(xs*xchan)+'~'+str(((xs+1)*xchan)-1)
                            if xs==0:
                                gspwi='0:'+str(0)+'~'+str(((xs+1)*xchan)-1)
                            else:
                                gspwi='0:'+str(0)+'~'+str(xchan-1)
                        if mynchani<=xchan:
                            spwi='0:'+str(xs*xchan)+'~'+str((xs*xchan)+mynchani-1)
                            gspwi='0:'+str(0)+'~'+str(mynchani-1)
                        gainsplitspw.append(gspwi)
                        msspw.append(spwi)
                        mynchani=mynchani-xchan
                        myfilei="msimg"+str(xs)+".ms"
                        xs=xs+1
                        splitspw.append(myfilei)
                print(gainsplitspw)
                print(msspw)
                print(splitspw)
                for numspw in range(0,len(msspw)):
                        default(mstransform)
                        mstransform(vis=myfile[0],outputvis=splitspw[numspw],spw=msspw[numspw],chanaverage=False,datacolumn='all',realmodelcol=True)
                os.system("rm -r"+" old"+myfile[0])
                os.system("rm -r"+" old"+myfile[0]+".flagversions")
                os.system("mv "+myfile[0]+".flagversions old"+myfile[0]+".flagversions")
                os.system("mv  "+myfile[0]+" old"+myfile[0])
                default(concat)
                concat(vis=splitspw,concatvis=myfile[0])
        mygainspw2=gainsplitspw
        return mygainspw2, msspw

def mysubbandselfcal(myfile,subbandchan,myref,nloops,nploops,myvalinit,mycellsize,myimagesize,mynterms2,mywproj1,mysolint1,myclipresid,myflagspw,mygainspw2,mymakedirty,niterstart,msspw,clean_robust):
	myref = myref
	nscal = nloops # number of selfcal loops
	npal = nploops # number of phasecal loops
	# selfcal loop
	myimages=[]
	mygt=[]
	myniterstart = niterstart
	myniterend = 200000	
	if nscal == 0:
		i = nscal
		myniter = 0 # this is to make a dirty image
		mythresh = str(myvalinit/(i+1))+'mJy'
		print("Using "+ myfile[i]+" for making only an image.")
		if usetclean == False:
			myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # clean
		else:
			myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # tclean
                if mynterms2 > 1:
                        exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
                else:
                        exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')

	else:
		for i in range(0,nscal+1): # plan 4 P and 4AP iterations
			if mymakedirty == True:
				if i == 0:
					myniter = 0 # this is to make a dirty image
					mythresh = str(myvalinit/(i+1))+'mJy'
					print("Using "+ myfile[i]+" for making only a dirty image.")
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # clean
					else:
						myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')

			else:
				myniter=int(myniterstart*2**i) #myniterstart*(2**i)  # niter is doubled with every iteration int(startniter*2**count)
				if myniter > myniterend:
					myniter = myniterend
				mythresh = str(myvalinit/(i+1))+'mJy'
				if i < npal:
					mypap = 'p'
#					print("Using "+ myfile[i]+" for imaging.")
					try:
						assert os.path.isdir(myfile[i])
					except AssertionError:
						logging.info("The MS file not found for imaging.")
						sys.exit()
					logging.info("Using "+ myfile[i]+" for imaging.")
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # clean
					else:
						myimg = mysbtclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')
					myimages.append(myimg)	# list of all the images created so far
					flagresidual(myfile[i],clipresid,'')
                                        if i>0:
                                                #myctables = mygaincal_ap(myfile[i],myref,mygt[i-1],i,mypap,mysolint1,uvrascal,mygainspw2)
                                                myctables = mygaincal_ap(myfile[i],myref,i,mypap,mysolint1,uvrascal,mygainspw2)
                                        else:
                                                #myctables = mygaincal_ap(myfile[i],myref,mygt,i,mypap,mysolint1,uvrascal,mygainspw2)
                                                myctables = mygaincal_ap(myfile[i],myref,i,mypap,mysolint1,uvrascal,mygainspw2)
                                        mygt.append(myctables)
                                        if i < nscal+1:
                                                myapplycal(myfile[i],mygt[i])
                                                myoutfile= mysplit(myfile[i],i)
                                                myfile.append(myoutfile)
######################old code###################################                                        
#					if i>0 and i<nscal+1:
#						for xgt in range(0,len(msspw)):	
#							myctables = mysbgaincal_ap(myfile[i],xgt,myref,mygt[i-1],i,mypap,mysolint1,'',mygainspw2)
#							mysbapplycal(myfile[i],myctables,xgt)
#							mygt.append(myctables) # full list of gaintables							
#					else:				
#						for xgt in range(0,len(msspw)):	
#							myctables = mysbgaincal_ap(myfile[i],xgt,myref,mygt,i,mypap,mysolint1,'',mygainspw2)		
#							mysbapplycal(myfile[i],myctables,xgt)	
#							mygt.append(myctables) # full list of gaintables
##################################################################
#					if i < nscal+1:
#						myoutfile= mysbsplit(myfile[i],i)
#						myfile.append(myoutfile)

				else:
					mypap = 'ap'
#					print("Using "+ myfile[i]+" for imaging.")
                                        try:
                                                assert os.path.isdir(myfile[i])
                                        except AssertionError:
                                                logging.info("The MS file not found for imaging.")
                                                sys.exit()
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # clean
					else:
						myimg = mysbtclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')
					myimages.append(myimg)	# list of all the images created so far
					flagresidual(myfile[i],clipresid,'')
                                        if i!=nscal:
                                                #myctables = mygaincal_ap(myfile[i],myref,mygt[i-1],i,mypap,mysolint1,'',mygainspw2)
                                                myctables = mygaincal_ap(myfile[i],myref,i,mypap,mysolint1,'',mygainspw2)
                                                mygt.append(myctables) # full list of gaintables
                                                if i < nscal+1:
                                                        myapplycal(myfile[i],mygt[i])
                                                        myoutfile= mysplit(myfile[i],i)
                                                        myfile.append(myoutfile)
###################old code##########################                                        
#					if i!= nscal:
#						for xgt in range(0,len(msspw)):	
#							myctables = mysbgaincal_ap(myfile[i],xgt,myref,mygt[i-1],i,mypap,mysolint1,'',mygainspw2)		
#							mysbapplycal(myfile[i],myctables,xgt)	
#						if i < nscal+1:
#							myoutfile= mysbsplit(myfile[i],i)
#							myfile.append(myoutfile)
#####################################################
#				print("Visibilities from the previous selfcal will be deleted.")
				logging.info("Visibilities from the previous selfcal will be deleted.")
				if i < nscal:
					fprefix = getfields(myfile[i])[0]
					myoldvis = fprefix+'-selfcal'+str(i-1)+'.ms'
#					print("Deleting "+str(myoldvis))
					logging.info("Deleting "+str(myoldvis))
					os.system('rm -rf '+str(myoldvis))
#			print('Ran the selfcal loop')
	return myfile, mygt, myimages



def flagsummary(myfile):
	try:
		assert os.path.isdir(myfile), "The MS file was not found."
	except AssertionError:
		logging.info("The MS file was not found.")
		sys.exit()
        s = flagdata(vis=myfile, mode='summary')
        allkeys = s.keys()
	logging.info("Flagging percentage:")
        for x in allkeys:
                try:
                        for y in s[x].keys():
                                flagged_percent = 100.*(s[x][y]['flagged']/s[x][y]['total'])
#                                logging.info(x, y, "%0.2f" % flagged_percent, "% flagged.")
				logstring = str(x)+' '+str(y)+' '+str(flagged_percent)
                                logging.info(logstring)
                except AttributeError:
                        pass

##########################################3

def midsubbandselfcal(myfile,subbandchan,myref,nloops,nploops,myvalinit,mycellsize,myimagesize,mynterms2,mywproj1,mysolint1,myclipresid,myflagspw,mygainspw2,mymakedirty,niterstart,msspw,clean_robust,midselfcal,scalno,mygt):
	myref = myref
	nscal = nloops # number of selfcal loops
	npal = nploops # number of phasecal loops
	# selfcal loop
	myimages=[]
#	mygt=[]
	myniterstart = niterstart
	myniterend = 200000	
	if nscal == 0:
		i = nscal
		myniter = 0 # this is to make a dirty image
		mythresh = str(myvalinit/(i+1))+'mJy'
		print("Using "+ myfile[i]+" for making only an image.")
		if usetclean == False:
			myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # clean
		else:
			myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # tclean
                if mynterms2 > 1:
                        exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
                else:
                        exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')

	else:
		for i in range(0,nscal+1): # plan 4 P and 4AP iterations
			if mymakedirty == True:
				if i == 0:
					myniter = 0 # this is to make a dirty image
					mythresh = str(myvalinit/(i+1))+'mJy'
					print("Using "+ myfile[i]+" for making only a dirty image.")
					if usetclean == False:
						myimg = myonlyclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # clean
					else:
						myimg = mytclean(myfile[i],myniter,mythresh,i,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')

			else:
				if midselfcal == True:
					j = scalno + i
                                        print(j)
				else:
					j = i
				myniter=int(myniterstart*2**j) #myniterstart*(2**i)  # niter is doubled with every iteration int(startniter*2**count)
				if myniter > myniterend:
					myniter = myniterend
				mythresh = str(myvalinit/(j+1))+'mJy'
                                print(j)
				if j <= npal:
					mypap = 'p'
#					print("Using "+ myfile[i]+" for imaging.")
					try:
						assert os.path.isdir(myfile[j])
					except AssertionError:
						logging.info("The MS file not found for imaging.")
						sys.exit()
					logging.info("Using "+ myfile[j]+" for imaging.")
					if usetclean == False:
						myimg = myonlyclean(myfile[j],myniter,mythresh,j,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # clean
					else:
						myimg = mysbtclean(myfile[j],myniter,mythresh,j,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')
					myimages.append(myimg)	# list of all the images created so far
					flagresidual(myfile[j],clipresid,'')
                                        if j>0:
                                                #myctables = mygaincal_ap(myfile[j],myref,mygt[j-1],j,mypap,mysolint1,uvrascal,mygainspw2)
                                                myctables = mygaincal_ap(myfile[j],myref,j,mypap,mysolint1,uvrascal,mygainspw2)
                                        else:
                                                #myctables = mygaincal_ap(myfile[j],myref,mygt,j,mypap,mysolint1,uvrascal,mygainspw2)
                                                myctables = mygaincal_ap(myfile[j],myref,j,mypap,mysolint1,uvrascal,mygainspw2)
                                        mygt.append(myctables)
                                        if j < nscal+1:
                                                myapplycal(myfile[j],mygt[j])
                                                myoutfile= mysplit(myfile[j],j)
                                                myfile.append(myoutfile)
                                elif j > npal:
					mypap = 'ap'
                                        print(j)
					print("Using "+ myfile[j]+" for imaging.")
                                        try:
                                                assert os.path.isdir(myfile[j])
                                        except AssertionError:
                                                logging.info("The MS file not found for imaging.")
                                                sys.exit()
					if usetclean == False:
						myimg = myonlyclean(myfile[j],myniter,mythresh,j,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # clean
					else:
						myimg = mysbtclean(myfile[j],myniter,mythresh,j,mycellsize,myimagesize,mynterms2,mywproj1,clean_robust)   # tclean
					if mynterms2 > 1:
						exportfits(imagename=myimg+'.image.tt0', fitsimage=myimg+'.fits')
					else:
						exportfits(imagename=myimg+'.image', fitsimage=myimg+'.fits')
					myimages.append(myimg)	# list of all the images created so far
					flagresidual(myfile[j],clipresid,'')
                                        if j!=nscal:
                                                #myctables = mygaincal_ap(myfile[j],myref,mygt[j-1],j,mypap,mysolint1,'',mygainspw2)
                                                myctables = mygaincal_ap(myfile[j],myref,j,mypap,mysolint1,'',mygainspw2)
                                                mygt.append(myctables) # full list of gaintables
                                                if j < nscal+1:
                                                        myapplycal(myfile[j],mygt[j])
                                                        myoutfile= mysplit(myfile[j],j)
                                                        myfile.append(myoutfile)
#				logging.info("Visibilities from the previous selfcal will be deleted.")
        			if j < nscal:
	        			fprefix = getfields(myfile[j])[0]
		        		myoldvis = fprefix+'-selfcal'+str(j-1)+'.ms'
                                        logging.info(i,j,myoldvis)
#					print("Deleting "+str(myoldvis))
#					logging.info("Deleting "+str(myoldvis))
#					os.system('rm -rf '+str(myoldvis))
#			print('Ran the selfcal loop')
	return myfile, mygt, myimages

#################additions for polarization calibration##############



#setjy(vis=ms, field = polcalib, spw = flagspw, scalebychan=True, standard='manual', 
#      fluxdensity=[i0,0,0,0], spix=[alpha,0], reffreq=reffreq, polindex=[c0,c1], polangle=[d0,d1])



def polsetjy(msfile,polcalib,flagspw,iflux,palpha,preffreq,ppolindex,ppolangle):
	default(setjy)
	setjy(vis=msfile, field = polcalib, spw = flagspw, scalebychan=True, standard='manual', 
	      fluxdensity=[iflux,0,0,0], spix=[palpha,0], reffreq=preffreq, polindex=ppolindex, polangle=ppolangle)

#gaincal(vis=ms, caltable = kcross1, field = polcalib, spw = flagspw, 
#        refant = refant, solint = 'inf', gaintype = 'KCROSS', combine = 'scan',
#        gaintable = [kcorrfile, bpassfile, gainfile], gainfield = [kcorrfield,bpassfield,polcalib],
#        parang = True) 

def kcrossgcal(ms,polcalib,flagspw,refant,kcorrfile,bpassfile,gainfile,kcorrfield,bpassfield):
	kcross1 = polcalib+'kcross1'
	default(gaincal)
	gaincal(vis=ms, caltable = kcross1, field = polcalib, spw = flagspw, 
        	refant = refant, solint = 'inf', gaintype = 'KCROSS', combine = 'scan',refantmode ='strict',
        	gaintable = [kcorrfile, bpassfile, gainfile], gainfield = [kcorrfield,bpassfield,polcalib], 
        	parang = True)
	return kcross1

def kcrossgcal1(ms,polcalib,flagspw,refant,gtables,gfields):
        kcross1 = polcalib+'kcross1'
        default(gaincal)
        gaincal(vis=ms, caltable = kcross1, field = polcalib, spw = flagspw, 
                refant = refant, solint = 'inf', gaintype = 'KCROSS', combine = 'scan',refantmode ='strict',
                gaintable = gtables, gainfield = gfields,
                parang = True)
        return kcross1


#polcal(vis=ms, caltable = leakage1, field = unpolcalib, spw = flagspw, 
#        refant = refant, solint = 'inf', poltype = 'Df', combine = 'scan',
#        gaintable = [kcorrfile, bpassfile, gainfile, kcross1], gainfield = [kcorrfield,bpassfield,unpolcalib,polcalib])

##polcal(vis=ms, caltable = leakage1, field = polcalib, spw = flagspw, 
##        refant = refant, solint = 'inf', poltype = 'Df+QU', combine = 'scan',
##        gaintable = [kcorrfile, bpassfile, gainfile, kcross1], gainfield = [kcorrfield,bpassfield,polcalib,polcalib])

#polcal(vis=ms, caltable = polang1, field = polcalib, refant = refant, solint = 'inf', poltype = 'Xf', 
#       combine = 'scan', gaintable = [kcorrfile, bpassfile, gainfile, kcross1, leakage1], 
#        gainfield = [kcorrfield,bpassfield,polcalib,polcalib,unpolcalib])


def polcalleakage(ms,unpolcalib,flagspw,refant,gtables,gfields):
	# leakage with unpolcal
	unpolcals =['3C84','OQ208']
	polcals = ['3C286','3C138']
	if unpolcalib in unpolcals:
		leakage1 = unpolcalib+'unpol-df'
		default(polcal)
		polcal(vis=ms, caltable = leakage1, field = unpolcalib, spw = flagspw, 
        		refant = refant, solint = 'inf', poltype = 'Df', combine = 'scan',
        		gaintable = gtables, gainfield = gfields)
	elif unpolcalib in polcals:
		leakage1 = unpolcalib+'pol-df-qu'
		default(polcal)
		polcal(vis=ms, caltable = leakage1, field = unpolcalib, spw = flagspw, 
        		refant = refant, solint = 'inf', poltype = 'Df+QU', combine = 'scan', 
        		gaintable = gtables, gainfield = gfields)
	return leakage1

# update after leakage cal done
#gaintable = [kcorrfile, bpassfile, gainfile, kcross1, leakage1], 
#gainfield = [kcorrfield,bpassfield,polcalib,polcalib,unpolcalib])

def polcalcross(ms, polcalib,refant, gtables, gfields):
	polang1 = polcalib+'angtab'
	default(polcal)
	polcal(vis=ms, caltable = polang1, field = polcalib, refant = refant, solint = 'inf', poltype = 'Xf', 
	       combine = 'scan', gaintable = gtables,
        	gainfield = gfields)
	return polang1


#print " starting fluxscale -> %s" % fluxfile 
#fluxscale(vis=ms, caltable = gainfile, reference = [fluxfield], 
#          transfer = [transferfield], fluxtable = fluxfile, 
#          listfile = ms+'.fluxscale.txt2',
#          append = False)


def pfluxscale(ms,gainfile,fluxfield,transferfield,fluxfile):
	default(fluxscale)
	fluxscale(vis=ms, caltable = gainfile, reference = [fluxfield], 
	          transfer = [transferfield], fluxtable = fluxfile, 
        	  listfile = ms+'.fluxscale.txt2',
        	  append = False)
	return fluxfile


#print " applying calibrations: primary calibrator"
#applycal(vis=ms, field = fluxfield, spw = flagspw, selectdata=False, calwt = False,
#    gaintable = [kcorrfile,bpassfile, fluxfile, kcross1, leakage1, polang1],
#    gainfield = [kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib],
#    parang = True)

#gaintable = [kcorrfile,bpassfile, fluxfile, kcross1, leakage1, polang1],
#gainfield = [kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib]

#



#def papplycal_fcal(ms,fluxfield,flagspw,gtables,gfields):
#	default(applycal)
#	applycal(vis=ms, field = fluxfield, spw = flagspw, selectdata=False, calwt = False,
#		    gaintable = [kcorrfile,bpassfile, fluxfile, kcross1, leakage1, polang1],
#		    gainfield = [kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib],
#		    parang = True)


def papplycal_fcal(ms,fluxfield,flagspw,gtables,gfields):
	default(applycal)
	applycal(vis=ms, field = fluxfield, spw = flagspw, selectdata=False, calwt = False,
		    gaintable = gtables, #[kcorrfile,bpassfile, fluxfile, kcross1, leakage1, polang1]
		    gainfield = gfields, #[kcorrfield,bpassfield,fluxfield, polcalib, unpolcalib, polcalib]
		    parang = True)


#print " applying calibrations: secondary calibrators"
#applycal(vis=ms, field = secondaryfield, spw = flagspw, selectdata = False, calwt = False,
#    gaintable = [kcorrfile, bpassfile, fluxfile, kcross1, leakage1, polang1],
#    gainfield = [kcorrfield, bpassfield,secondaryfield, polcalib, unpolcalib, polcalib],
#    parang= True)

#gaintable = [kcorrfile, bpassfile, fluxfile, kcross1, leakage1, polang1],
#gainfield = [kcorrfield, bpassfield,secondaryfield, polcalib, unpolcalib, polcalib],

def papplycal_pcal(ms,secondaryfield,flagspw,gtables,gfields):
	default(applycal)
	applycal(vis=ms, field = fluxfield, spw = flagspw, selectdata=False, calwt = False,
		    gaintable = gtables,
		    gainfield = gfields,
		    parang = True)


#print " applying calibrations: polarized calibrator"
#applycal(vis=ms, field = polcalib, spw = flagspw, selectdata = False, calwt = False,
#    gaintable = [kcorrfile, bpassfile, fluxfile, kcross1, leakage1, polang1],
#    gainfield = [kcorrfield, bpassfield,polcalib, polcalib, unpolcalib, polcalib],
#    parang= True)

#gaintable = [kcorrfile, bpassfile, fluxfile, kcross1, leakage1, polang1],
#gainfield = [kcorrfield, bpassfield,polcalib, polcalib, unpolcalib, polcalib],


def papplycal_polcal(ms,polcalib,flagspw,gtables,gfields):
	default(applycal)
	applycal(vis=ms, field = polcalib, spw = flagspw, selectdata = False, calwt = False,
		    gaintable = gtables,
		    gainfield = gfields,
		    parang= True)


#print " applying calibrations: unpolarized calibrator"
#applycal(vis=ms, field = unpolcalib, spw = flagspw, selectdata = False, calwt = False,
#    gaintable = [kcorrfile, bpassfile, fluxfile, kcross1, leakage1, polang1],
#    gainfield = [kcorrfield, bpassfield,unpolcalib, polcalib, unpolcalib, polcalib],
#    parang= True)

#gaintable = [kcorrfile, bpassfile, fluxfile, kcross1, leakage1, polang1],
#gainfield = [kcorrfield, bpassfield,unpolcalib, polcalib, unpolcalib, polcalib]

def papplycal_unpolcal(ms,unpolcalib,flagspw,gtables,gfields):
	default(applycal)
	applycal(vis=ms, field = unpolcalib, spw = flagspw, selectdata = False, calwt = False,
		    gaintable = gtables,
		    gainfield = gfields,
		    parang= True)


#print " applying calibrations: target fields"
#applycal(vis=ms, field = target, spw = flagspw, selectdata = False, calwt = False,
#    gaintable = [kcorrfile, bpassfile, fluxfile, kcross1, leakage1, polang1],
#    gainfield = [kcorrfield, bpassfield,secondaryfield, polcalib, unpolcalib, polcalib],
#    parang= True)

#gaintable = [kcorrfile, bpassfile, fluxfile, kcross1, leakage1, polang1],
#gainfield = [kcorrfield, bpassfield,secondaryfield, polcalib, unpolcalib, polcalib]

def papplycal_target(ms,target,flagspw,gtables,gfields):
	default(applycal)
	applycal(vis=ms, field = target, spw = flagspw, selectdata = False, calwt = False,
		    gaintable = gtables,
		    gainfield = gfields,
		    parang= True)


###########################################
############## write imaging functions for Stokes Q,U and V images ##############


def tcleanQ(msfile,cell,imsize, mynterms1,mywproj,clean_robust):
        qimagename=msfile+".Q"
        if mynterms1 >1:
                default(tclean)
                tclean(vis=msfile,selectdata=True,field="",spw="",timerange="",
                        uvrange="",antenna="",scan="",observation="",intent="",
                       datacolumn="corrected",imagename=qimagename,imsize=imsize,cell=cell,phasecenter="",
                       stokes="Q",projection="SIN",startmodel="",specmode="mfs",reffreq="",
                       nchan=-1,start="",width="",outframe="LSRK",veltype="radio",
                       restfreq=[],interpolation="linear",perchanweightdensity=False,gridder="widefield",facets=1,
                       psfphasecenter="",chanchunks=1,wprojplanes=mywproj,vptable="",usepointing=False,
                       mosweight=True,aterm=True,psterm=False,wbawp=True,conjbeams=False,
                       cfcache="",computepastep=360.0,rotatepastep=360.0,pblimit=-0.001,normtype="flatnoise",
                       deconvolver="mtmfs",scales=[],nterms=mynterms1,smallscalebias=0.6,restoration=True,
                       restoringbeam=[],pbcor=False,outlierfile="",weighting="briggs",robust=clean_robust,
                       noise="1.0Jy",npixels=0,uvtaper=[],niter=50000,gain=0.1,
                       threshold="0.001mJy",nsigma=0.0,cycleniter=-1,cyclefactor=1.1,minpsffraction=0.05,
                       maxpsffraction=0.8,interactive=False,usemask="auto-multithresh",mask="",pbmask=0.0,
                       sidelobethreshold=2.0,noisethreshold=5.0,lownoisethreshold=1.5,negativethreshold=0.0,smoothfactor=1.0,
                       minbeamfrac=0.3,cutthreshold=0.01,growiterations=75,dogrowprune=True,minpercentchange=-1.0,
                       verbose=False,fastnoise=True,restart=True,savemodel="modelcolumn",calcres=True,
                       calcpsf=True,parallel=False)
                default(exportfits)
                exportfits(imagename=qimagename+".image.tt0",fitsimage=qimagename+"-Q.fits",velocity=False,optical=False,bitpix=-32,
                   minpix=0,maxpix=-1,overwrite=False,dropstokes=False,stokeslast=True,
                   history=True,dropdeg=False)
        else:
                tclean(vis=msfile,
                        imagename=qimagename, selectdata= True, field='', spw='', imsize=imsize, cell=cell, robust=clean_robust, weighting='briggs',
                        stokes="Q", specmode='mfs',     nterms=mynterms1, niter=50000, usemask='auto-multithresh',minbeamfrac=0.1,sidelobethreshold = 2.0,
                        smallscalebias=0.6, threshold= mythresh, aterm =True, pblimit=-1,
                        deconvolver='clark', gridder='wproject', wprojplanes=mywproj, scales=[],wbawp=False,
                        restoration = True, savemodel='modelcolumn', cyclefactor = 0.5, parallel=False,
                        interactive=False)
                default(exportfits)
                exportfits(imagename=qimagename+".image",fitsimage=qimagename+"-Q.fits",velocity=False,optical=False,bitpix=-32,
                   minpix=0,maxpix=-1,overwrite=False,dropstokes=False,stokeslast=True,
                   history=True,dropdeg=False)
        return qimagename


def tcleanU(msfile,cell,imsize, mynterms1,mywproj,clean_robust):
        Uimagename=msfile+".U"
        if mynterms1 >1:
                default(tclean)
                tclean(vis=msfile,selectdata=True,field="",spw="",timerange="",
                        uvrange="",antenna="",scan="",observation="",intent="",
                       datacolumn="corrected",imagename=Uimagename,imsize=imsize,cell=cell,phasecenter="",
                       stokes="U",projection="SIN",startmodel="",specmode="mfs",reffreq="",
                       nchan=-1,start="",width="",outframe="LSRK",veltype="radio",
                       restfreq=[],interpolation="linear",perchanweightdensity=False,gridder="widefield",facets=1,
                       psfphasecenter="",chanchunks=1,wprojplanes=mywproj,vptable="",usepointing=False,
                       mosweight=True,aterm=True,psterm=False,wbawp=True,conjbeams=False,
                       cfcache="",computepastep=360.0,rotatepastep=360.0,pblimit=-0.001,normtype="flatnoise",
                       deconvolver="mtmfs",scales=[],nterms=mynterms1,smallscalebias=0.6,restoration=True,
                       restoringbeam=[],pbcor=False,outlierfile="",weighting="briggs",robust=clean_robust,
                       noise="1.0Jy",npixels=0,uvtaper=[],niter=50000,gain=0.1,
                       threshold="0.001mJy",nsigma=0.0,cycleniter=-1,cyclefactor=1.1,minpsffraction=0.05,
                       maxpsffraction=0.8,interactive=False,usemask="auto-multithresh",mask="",pbmask=0.0,
                       sidelobethreshold=2.0,noisethreshold=5.0,lownoisethreshold=1.5,negativethreshold=0.0,smoothfactor=1.0,
                       minbeamfrac=0.3,cutthreshold=0.01,growiterations=75,dogrowprune=True,minpercentchange=-1.0,
                       verbose=False,fastnoise=True,restart=True,savemodel="modelcolumn",calcres=True,
                       calcpsf=True,parallel=False)
                default(exportfits)
                exportfits(imagename=Uimagename+".image.tt0",fitsimage=Uimagename+"-U.fits",velocity=False,optical=False,bitpix=-32,
                   minpix=0,maxpix=-1,overwrite=False,dropstokes=False,stokeslast=True,
                   history=True,dropdeg=False)
        else:
                tclean(vis=msfile,
                        imagename=Uimagename, selectdata= True, field='', spw='', imsize=imsize, cell=cell, robust=clean_robust, weighting='briggs',
                        stokes="U", specmode='mfs',     nterms=mynterms1, niter=50000, usemask='auto-multithresh',minbeamfrac=0.1,sidelobethreshold = 2.0,
                        smallscalebias=0.6, threshold= mythresh, aterm =True, pblimit=-1,
                        deconvolver='clark', gridder='wproject', wprojplanes=mywproj, scales=[],wbawp=False,
                        restoration = True, savemodel='modelcolumn', cyclefactor = 0.5, parallel=False,
                        interactive=False)
                default(exportfits)
                exportfits(imagename=Uimagename+".image",fitsimage=Uimagename+"-U.fits",velocity=False,optical=False,bitpix=-32,
                   minpix=0,maxpix=-1,overwrite=False,dropstokes=False,stokeslast=True,
                   history=True,dropdeg=False)
        return Uimagename


def tcleanV(msfile,cell,imsize, mynterms1,mywproj,clean_robust):
        Vimagename=msfile+".V"
        if mynterms1 >1:
                default(tclean)
                tclean(vis=msfile,selectdata=True,field="",spw="",timerange="",
                        uvrange="",antenna="",scan="",observation="",intent="",
                       datacolumn="corrected",imagename=Vimagename,imsize=imsize,cell=cell,phasecenter="",
                       stokes="V",projection="SIN",startmodel="",specmode="mfs",reffreq="",
                       nchan=-1,start="",width="",outframe="LSRK",veltype="radio",
                       restfreq=[],interpolation="linear",perchanweightdensity=False,gridder="widefield",facets=1,
                       psfphasecenter="",chanchunks=1,wprojplanes=mywproj,vptable="",usepointing=False,
                       mosweight=True,aterm=True,psterm=False,wbawp=True,conjbeams=False,
                       cfcache="",computepastep=360.0,rotatepastep=360.0,pblimit=-0.001,normtype="flatnoise",
                       deconvolver="mtmfs",scales=[],nterms=mynterms1,smallscalebias=0.6,restoration=True,
                       restoringbeam=[],pbcor=False,outlierfile="",weighting="briggs",robust=clean_robust,
                       noise="1.0Jy",npixels=0,uvtaper=[],niter=50000,gain=0.1,
                       threshold="0.001mJy",nsigma=0.0,cycleniter=-1,cyclefactor=1.1,minpsffraction=0.05,
                       maxpsffraction=0.8,interactive=False,usemask="auto-multithresh",mask="",pbmask=0.0,
                       sidelobethreshold=2.0,noisethreshold=5.0,lownoisethreshold=1.5,negativethreshold=0.0,smoothfactor=1.0,
                       minbeamfrac=0.3,cutthreshold=0.01,growiterations=75,dogrowprune=True,minpercentchange=-1.0,
                       verbose=False,fastnoise=True,restart=True,savemodel="modelcolumn",calcres=True,
                       calcpsf=True,parallel=False)
                default(exportfits)
                exportfits(imagename=Vimagename+".image.tt0",fitsimage=Vimagename+"-V.fits",velocity=False,optical=False,bitpix=-32,
                   minpix=0,maxpix=-1,overwrite=False,dropstokes=False,stokeslast=True,
                   history=True,dropdeg=False)
        else:
                tclean(vis=msfile,
                        imagename=Vimagename, selectdata= True, field='', spw='', imsize=imsize, cell=cell, robust=clean_robust, weighting='briggs',
                        stokes="V", specmode='mfs',     nterms=mynterms1, niter=50000, usemask='auto-multithresh',minbeamfrac=0.1,sidelobethreshold = 2.0,
                        smallscalebias=0.6, threshold= mythresh, aterm =True, pblimit=-1,
                        deconvolver='clark', gridder='wproject', wprojplanes=mywproj, scales=[],wbawp=False,
                        restoration = True, savemodel='modelcolumn', cyclefactor = 0.5, parallel=False,
                        interactive=False)
                default(exportfits)
                exportfits(imagename=Vimagename+".image",fitsimage=Vimagename+"-V.fits",velocity=False,optical=False,bitpix=-32,
                   minpix=0,maxpix=-1,overwrite=False,dropstokes=False,stokeslast=True,
                   history=True,dropdeg=False)
        return Vimagename




#############End of functions##############################################################################
