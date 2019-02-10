Fast algorithms for distinct levels for_10000 variables and_3 million records sort sql freq hash

UPDATES Paul Dorfman Paul-1 and Paul-2

   PAUL 1 -- Fast simple single HASH small memory solutions
   PAUL 2 -- Know your data and cadinality efects
             Limited-range [0:1000] Solution (small footprint)
             Array based Hashing and Accommodating any integer key range

   See Pauls impressive recent faster HASH solutions on the end of this post
   Paul Dorfman
   sashole@bellsouth.net

   Hope to do some serious benchmarking when I get back ti home base and the 'Beast' computer.

  I have no association with SAS, Amazon or Paul/Don enterprise. However I find their book to be
  a great reference on HASH algorithms. I use it as a reference, you don't need to read the
  entire text to find it useful.

  see
  https://www.oreilly.com/library/view/data-management-solutions/9781635260595/

  Distinct counts for 10,000 variables and 3 million reccords can be done in under 25 minutes with
  90 cores, 250gb of ram and average available system utilization of about 20%.
  This is what EG server should be designed for?

  It took 92 seconds to get count distinct on 1,000 varables and 3 million obs.

  I don't have enough ram. If I had 250gb ram and 16 cores and 250gb ram It would take
  about 5 minutes

  The HASH is the fastest

   Benchmarks for 125, 1,000 and 10,000 variables and 1,000 variables

        Seconds  Variables   Method
        ------   ---------   ------

     1.   88       125       Hash
     2.  354       125       Freq nlevels Outputs one dataset
     3.  925       125       Sortort nodupkey (need to count ouput recs and append )
     4.  112       125       Sql count(distinct x)

     5.   92     1,000       HASH 8 cores parallel uses 32gb of ram
     6.  124     1,000       SQL  8 cores Parallel count(distinct x) (use magic=103 for hash safer?)

     7.  460*   10,000       HASH 16 cores 256gb $2000 workstation (5 sets of 16 paralell tasks)
     8.  620*   10,000       HASH 16 cores 256gb workstation SQL   (5 sets of 16 paralell tasks)

     9   460*   10,000       HASH 80 cores 256gb at 20% cpu utilization
    10   620*   10,000       SQL  80 cores 256gb at 20% cpu utilization (Teradata, Exadata, SAS grid)

     * estimates

  github
  https://tinyurl.com/y8srtnsz
  https://github.com/rogerjdeangelis/utl-fast-algorithms-for-count-distinct-levels-for-10000-variables-and-3-million-records-sort-sql-fre

  This was run on my $1,000 Dell T7400 64gb ram dual 3ghz XEON with 8 cores and raid 0 SSD arrays

  inspired by
  https://communities.sas.com/t5/SAS-Programming/Distinct-values-for-all-columns-in-dataset/m-p/530918


INPUT
=====

* spread the data around (note Dell T7400 has a 1000 watt power supply so it supports many SSDs;
* 250gd SSDs at about $35 each;
* too lazy to try SPDE;

libname d1 "c:/wrk/";
libname d2 "d:/wrk/";
libname d3 "f:/wrk/";
libname d4 "g:/wrk/";
libname d5 "i:/wrk/";
libname d6 "f:/wrk/";
libname d7 "i:/wrk/";
libname d8 "d:/wrk/";

data
     d1.d1(keep=    num1-num125 )
     d2.d2(keep=  num126-num250 )
     d3.d3(keep=  num251-num375 )
     d4.d4(keep=  num376-num500 )
     d5.d5(keep=  num501-num625 )
     d6.d6(keep=  num626-num750 )
     d7.d7(keep=  num751-num875 )
     d8.d8(keep= num876-num1000 )
     ;
     retain seq;

     call streaminit(1234);

     array nums[1000] num1-num1000;

     do seq=1 to 3000000;
        do i=1 to 1000;
             select;
                when (mod(i,3)=0) nums[i]=int(100*rand('uniform'));
                when (mod(i,7)=0) nums[i]=int(200*rand('uniform'));
                otherwise nums[i]=int(1000*rand('uniform'));
             end;
        end;
        output;
     end;

     drop i;

run;quit;


Input Eight tables D1.D1 thru D8.D8 on various drives/arrays
------------------------------------------------------------

D1.D1 - Total Obs 3,000,000 Middle Observation(1500000 )

 -- NUMERIC --     TYPE   Typical Value
NUM1                 N8       853
NUM2                 N8       885
NUM3                 N8       355
...
NUM123               N8       319
NUM124               N8       374
NUM125               N8       290

..

D8.D8 - Total Obs 3,000,000 Middle Observation(1500000 )

 -- NUMERIC --     TYPE   Typical Value
NUM876               N8       123
NUM877               N8       987
NUM878               N8       553
...
NUM998               N8       931
NUM999               N8       474
NUM1000              N8       187

..

EXAMPLE OUTPUT ( 1,000 varibles HASH and SQL)
---------------------------------------------------

One Observation(1 ) of HASH ALL mrg1 - Total Obs 1

 Variables
                        Number
 -- NUMERIC --          Distint Levels

CNTNUM1        N8       1000
CNTNUM2        N8       1000
CNTNUM3        N8       100
...
CNTNUM998      N8       1000
CNTNUM999      N8       100
CNTNUM1000     N8       1000



PROCESS
=======

1. Hash
=======

filename ft15f001 "c:/oto/hashUnq.sas"; * thanks Data_null_;
parmcards4;
%macro hashUnq(drv,inp,beg,end);

    /*
    %let drv=c;
    %let inp=d1.d1;
    %let beg=1;
    %let end=5;
    */

    %utlnopts;

    libname %scan(&inp,1,%str(.)) "&drv.:/wrk/" filelockwait=10;

    libname sd1 "f:/sd1" filelockwait=10;

    sasfile &inp load;

    %let start=%sysfunc(time());
    data _null_;

      do i=&beg to &end;

         call symputx('i',i);

         call execute('
            data num&i;
              if _n_ = 1 then do;
                dcl hash h(dataset:"&inp.(keep=num&i)", duplicate: "r");
                h.defineKey("num&i");
                h.defineDone();
                call missing(num&i);
              end;
              cntnum&i = h.num_items;
              drop num&i;
              output;
              *rc=h.delete();
              stop;
            run;
         ');

      end;

      stop;

    run;quit;

    %array(nubs,values=num&beg.-num&end);

    data &inp.mrg&beg;
      merge
        %do_over(nubs,phrase=?)
      ;
    run;quit;

    %put elap=%sysevalf(%sysfunc(time()) - &start);

    sasfile &inp close;

%mend hashUnq;
;;;;
run;quit;


* test interactively;
%utlopts;
proc datasets lib=work kill;
run;quit;
%symdel inp beg end;
%hashUnq(i,d5.d5,501,502);
%hashUnq(d,d8.d8,876,886);

* pause for a second between submission. May not be needed;
%macro paus;
 data _null_;
   put "begin";
   call sleep(1,1);
   put "end";
 run;quit;
%mend paus;


%let _s=%sysfunc(compbl(C:\Progra~1\SASHome\SASFoundation\9.4\sas.exe -sysin c:\nul -rsasuser
  -sasautos c:\oto -work i:\wrk -config c:\cfg\cfgsas.cfg));

%utlopts;
* set up;

systask kill sys1 sys2 sys3 sys4 sys5 sys6 sys7 sys8;
systask command "&_s -termstmt %nrstr(%hashUnq(c,d1.d1,1,125);) -log d:\log\a1.log" taskname=sys1; %paus;
systask command "&_s -termstmt %nrstr(%hashUnq(d,d2.d2,126,250);) -log d:\log\a2.log" taskname=sys2; %paus;
systask command "&_s -termstmt %nrstr(%hashUnq(f,d3.d3,251,375);) -log d:\log\a3.log" taskname=sys3; %paus;
systask command "&_s -termstmt %nrstr(%hashUnq(g,d4.d4,376,500);) -log d:\log\a4.log" taskname=sys4; %paus;
systask command "&_s -termstmt %nrstr(%hashUnq(i,d5.d5,501,625);) -log d:\log\a5.log" taskname=sys5; %paus;
systask command "&_s -termstmt %nrstr(%hashUnq(f,d6.d6,626,750);) -log d:\log\a6.log" taskname=sys6; %paus;
systask command "&_s -termstmt %nrstr(%hashUnq(i,d7.d7,751,875);) -log d:\log\a7.log" taskname=sys7; %paus;
systask command "&_s -termstmt %nrstr(%hashUnq(d,d8.d8,876,1000);) -log d:\log\a8.log" taskname=sys8; %paus;

* there appears to be a bug with wait for it termonates some tasks prematurely?;
*waitfor sys1 sys2 sys3 sys4 sys5 sys6 sys7 sys8;
systask kill sys1 sys2 sys3 sys4 sys5 sys6 sys7 sys8;

/* task 8 of 8 patial log
247   +    data num999;
            if _n_ = 1 then do;
              dcl hash
              h(dataset:"d8.d8(keep=num999)", duplicate: "r");
              h.defineKey("num999");
              h.defineDone();
              call missing(num999);
           end;
248   +    cntnum999 = h.num_items;
           drop num999;
           output;
           *rc=h.delete();
           stop;
           run;
NOTE: There were 3000000 observations read from the data set D8.D8.
NOTE: The data set WORK.NUM999 has 1 observations and 1 variables.
NOTE: DATA statement used (Total process time):
      real time           0.67 seconds
249   +    data num1000;
            if _n_ = 1 then do;
              dcl hash
              h(dataset:"d8.d8(keep=num1000)", duplicate: "r");
              h.defineKey("num1000");
              h.defineDone();
              call missing(num1000);
           end;
250   +    cntnum1000 = h.num_items;
           drop num1000;
           output;
           *rc=h.delete();
           stop;
           run;
NOTE: There were 3000000 observations read from the data set D8.D8.
NOTE: The data set WORK.NUM1000 has 1 observations and 1 variables.
NOTE: DATA statement used (Total process time):
      real time           0.71 seconds
*/

data hashAll;
  merge
    d1.d1mrg1
    d2.d2mrg126
    d3.d3mrg251
    d4.d4mrg376
    d5.d5mrg501
    d6.d6mrg626
    d7.d7mrg751
    d8.d8mrg876
  ;
run;quit;


NOTE: There were 1 observations read from the data set D1.D1MRG1.
NOTE: There were 1 observations read from the data set D2.D2MRG126.
NOTE: There were 1 observations read from the data set D3.D3MRG251.
NOTE: There were 1 observations read from the data set D4.D4MRG376.
NOTE: There were 1 observations read from the data set D5.D5MRG501.
NOTE: There were 1 observations read from the data set D6.D6MRG626.
NOTE: There were 1 observations read from the data set D7.D7MRG751.
NOTE: There were 1 observations read from the data set D8.D8MRG876.
NOTE: The data set WORK.HASHALL has 1 observations and 1000 variables.
NOTE: DATA statement used (Total process time):
      real time           0.04 seconds
      user cpu time       0.00 seconds



2. Proc freq nlevels Outputs one dataset (only 125 variables)
--------------------------------------------------------------

ods exclude all;
ods output nlevels=d1.d1levels;
proc freq data=d1.d1 nlevels;
run;quit;
ods select all;

NOTE: The data set D1.D1LEVELS has 125 observations and 2 variables.
NOTE: There were 3000000 observations read from the data set D1.D1.
NOTE: PROCEDURE FREQ used (Total process time):
      real time           5:54.61
      user cpu time       5:49.41
      system cpu time     4.96 seconds
      memory              20061.87k
      OS Memory           40944.00k
      Timestamp           02/01/2019 07:11:15 AM
      Step Count                        26  Switch Count  9


3. Proc sort nodupkey (need to count ouput recs and append - very fast)
-----------------------------------------------------------------------

proc sort data=d1.d1 out=num1 nodupkey;
  by num1;
run;quit;

real time           7.18 seconds
...

proc sort data=d1.d1 out=num125 nodupkey;
  by num125;
run;quit;

real time           7.38 seconds

You now have to extract the number of obs
for each of the 125 tables.
But you hav already lost the race.

7.38*125 = 922.5 seconds


4. Proc sql count(distinct x) (even with hash ie magic 103 - one output dataset)
-----------------------------------------------------------------------

* do 125;

sasfile d1.d1 load;

%array(nubs,values=num1-num125);

proc sql ;
  create
     table sql125 as
  select
     %do_over(nubs,phrase= count(distinct ?) as ?,between=comma)
  from
     d1.d1
;quit;

* real time           1:22.52;


* 1,000 in parallel;

filename ft15f001 "c:/oto/sqlUnq.sas"; * thanks Data_null_;
parmcards4;
%macro sqlUnq(drv,inp,beg,end);

    /*
    %let inp=d1.d1;
    %let beg=1;
    %let end=5;
    */

    *options nothreads nosymbolgen nomlogic nomacrogen nosource nosource2 nonotes nofullstimer NOQUOTELENMAX;
    %utlopts;

    libname %scan(&inp,1,%str(.)) "&drv.:/wrk/" /*filelockwait=10*/;

    libname sd1 "f:/sd1" /*filelockwait=10*/;

      sasfile &inp load;

      %utlopts;
      *options nothreads  nosymbolgen nomacrogen nomlogic nosource nosource2 nonotes nofullstimer NOQUOTELENMAX;
      options replace;
      proc sql magic=103;
         create
           table sd1.sql&beg as
         select
           %do i=&beg %to %eval(&end -1);
             count(distinct num&i.) as num&i,
           %end;
           count(distinct num&i.) as num&i
         from
           &inp
      ;quit;


%mend sqlUnq;
;;;;
run;quit;

* test interactively;
%utlnopts;
proc datasets lib=work kill;
run;quit;
%symdel inp beg end;
%sqlUnq(c,d1.d1,1,2);


sd1.sql501

* pause for a second between submission. May not be needed;
%macro paus;
 data _null_;
   put "begin";
   call sleep(1,1);
   put "end";
 run;quit;
%mend paus;


%let _s=%sysfunc(compbl(C:\Progra~1\SASHome\SASFoundation\9.4\sas.exe -sysin c:\nul -rsasuser
  -sasautos c:\oto -work i:\wrk -config c:\cfg\cfgsas.cfg));

%utlopts;
* set up;

systask kill sys1 sys2 sys3 sys4 sys5 sys6 sys7 sys8;
systask command "&_s -termstmt %nrstr(%sqlUnq(c,d1.d1,1,125);) -log d:\log\a1.log" taskname=sys1; %paus;
systask command "&_s -termstmt %nrstr(%sqlUnq(d,d2.d2,126,250);) -log d:\log\a2.log" taskname=sys2; %paus;
systask command "&_s -termstmt %nrstr(%sqlUnq(f,d3.d3,251,375);) -log d:\log\a3.log" taskname=sys3; %paus;
systask command "&_s -termstmt %nrstr(%sqlUnq(g,d4.d4,376,500);) -log d:\log\a4.log" taskname=sys4; %paus;
systask command "&_s -termstmt %nrstr(%sqlUnq(i,d5.d5,501,625);) -log d:\log\a5.log" taskname=sys5; %paus;
systask command "&_s -termstmt %nrstr(%sqlUnq(f,d6.d6,626,750);) -log d:\log\a6.log" taskname=sys6; %paus;
systask command "&_s -termstmt %nrstr(%sqlUnq(i,d7.d7,751,875);) -log d:\log\a7.log" taskname=sys7; %paus;
systask command "&_s -termstmt %nrstr(%sqlUnq(d,d8.d8,876,1000);) -log d:\log\a8.log" taskname=sys8; %paus;

* there appears to be a bug with wait for it termonates some tasks prematurely?;
*waitfor sys1 sys2 sys3 sys4 sys5 sys6 sys7 sys8;
systask kill sys1 sys2 sys3 sys4 sys5 sys6 sys7 sys8;

/*
* Partial log task 1 of 8;
num95, count(distinct num96) as num96, count(distinct num97) as num97, count(distinct num98) as
num98, count(distinct num99) as num99, count(distinct num100) as num100, count(distinct num101)
as num101, count(distinct num102) as num102, count(distinct num103) as num103, count(distinct
num104) as num104, count(distinct num105) as num105, count(distinct num106) as num106,
count(distinct num107) as num107, count(distinct num108) as num108, count(distinct num109) as
num109, count(distinct num110) as num110, count(distinct num111) as num111, count(distinct
num112) as num112, count(distinct num113) as num113, count(distinct num114) as num114,
count(distinct num115) as num115, count(distinct num116) as num116, count(distinct num117) as
num117, count(distinct num118) as num118, count(distinct num119) as num119, count(distinct
num120) as num120, count(distinct num121) as num121, count(distinct num122) as num122,
count(distinct num123) as num123, count(distinct num124) as num124, count(distinct num125) as
num125 from d1.d1 ;
NOTE: Table SD1.SQL1 created, with 1 rows and 125 columns.
MPRINT(SQLUNQ):  quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           1:21.55
*/


*____             _   _
|  _ \ __ _ _   _| | / |
| |_) / _` | | | | | | |
|  __/ (_| | |_| | | | |
|_|   \__,_|\__,_|_| |_|

;


Very impressive indeed!

At the same time, looking at the task tells me again time how much
performance can be gained by taking advantage of the nature of the key
(know thy data, that is). To wit, your key as a limited-range [0:1000]
integer, hence the problem can be solved in one feel swoop by key-indexing
the counts into a temp array:

%let W =    1000 ; * variables ;
%let H =  300000 ; * records ;
%let R =    1000 ; * integer range ;

data have ;
  call streaminit (7) ;
  array nn num1-num&w ;
  do _n_ = 1 to &h ;
    do over nn ;
      nn = rand ("integer", &r) ;
    end ;
    output ;
  end ;
run ;

data want (keep = Nvar Count) ;
  array q [&w, 0:&r] _temporary_ ;
  set have end = end ;
  array nn num: ;
  do over nn ;
    q[_i_, nn] = 1 ;
  end ;
  if end ;
  call missing (of nn[*]) ;
  do over nn ;
    do _n_ = 0 to hbound (q, 2) ;
      nn ++ N (q[_i_,_n_]) ;
    end ;
    Nvar = put (vname (nn), $8.) ;
    Count = nn ;
    output ;
  end ;
run ;

This guy runs in 9 seconds flat and takes 85 Mb of RAM on my run-of-the mill
old 2.4 GHz ThinkPad W520 (X64_7PRO TS 1M4). Since in my tests, the code scales
strictly linearly by both W and H, it should take 90 seconds for W=1k and H=3m
or 900 seconds for W=10k and H=3m. The 900, of course, is about twice your
estimated 460, but the memory footprint is 0.85 Mb vs 32 Gb.

Of course, the limited integer key range is a specific situation. However,
if the key can have any range, but still an integer, the solution can be rendered
as array-based hashing, in which case the memory footprint is defined, not by the
key range, but by the number of its distinct values Q. So,  the RAM load for the
array-based hash table will be restricted to Q / 0.5 (hash load
factor) * 8 (bytes per array item) * W - i.e., with Q=2000 and W=10000 it will be
under 310 Mb. I'm not showing it here but from my experience, it will run about
20 percent slower than the key-indexed scheme above but with the sizable benefit
of accommodating any integer key range.
If the key is anything else - except for a simple character key shorter than $8
(which can be turned into an integer using the PIBw. informat without losing precision) -
there is essentially no choice but the hash object, most likely in the form you've shown
here. The reason for the latter is that logically pure straightforward single-pass hash
 object code has to rely on a hash-of-
hashes table in order to scroll through W hash instances for every input observation:

data want_hash (keep = Nvar Count) ;
  set have end = end ;
  array nn num: ;
  if _n_ = 1 then do ;
    dcl hash hh () ;
    hh.definekey ("_i_") ;
    hh.definedata ("h") ;
    hh.definedone () ;
    do over nn ;
      dcl hash h () ;
      h.definekey ("_n_") ;
      h.definedone () ;
      hh.add() ;
    end ;
  end ;
  do over nn ;
    hh.find() ;
    h.ref (key:nn, data:nn) ;
  end ;
  if end ;
  do over nn ;
    Nvar = put (vname (nn), $8.) ;
    hh.find() ;
    Count = h.num_items ;
    output ;
  end ;
run ;


*____             _   ____
|  _ \ __ _ _   _| | |___ \
| |_) / _` | | | | |   __) |
|  __/ (_| | |_| | |  / __/
|_|   \__,_|\__,_|_| |_____|

;
Your word "minimal" has made me think of whether using the hash of hashes approach
is really "minimal"; and I realized that it is "minimal" only in the sense that,
once I started thinking in this direction, it appeared to be the simplest, and so
what ended up being "minimal" was merely my tunnel vision thinking. At times,
following one's stream-of-the-consciousness isn't a bad thing,
and it often works - but rarely results in "minimal" coding or "maximal"
performance. After posting, I had been having a nagging suspicion that by
doing exactly that, I overcomplicated the thing, and the words of Roark said
to Keating "Simpler, Peter, simpler ..." kept ringing in my ears.

And, lo and behold, of course there's a much simpler way to solve the problem -
by using a *single* hash object instance. How? Well, simple:

%let W =  1000 ; * variables ;
%let H = 30000 ; * records ;
%let R =  1000 ; * key range = max N of unique values ;

data have ;
  call streaminit (7) ;
  array nn num1-num&w ;
  do _n_ = 1 to &h ;
    do over nn ;
      nn = 1e6 + rand ("integer", &r) ;
    end ;
    output ;
  end ;
run ;

data _null_ ;
  dcl hash h (ordered:"A") ;
  h.definekey  ("nvar", "_n_") ;
  h.definedata ("nvar") ;
  h.definedone () ;
  do until (end) ;
    set have end = end ;
    array nn (Nvar) num: ;
    do over nn ;
      h.ref (key:nvar, key:nn, data:nvar) ;
    end ;
  end ;
  h.output (dataset: "hash") ;
run ;

data want_one_hash ;
  do Count = 1 by 1 until (last.Nvar) ;
    set hash ;
    by Nvar ;
  end ;
run ;

Note that the final counting can be done using an iterator in the first step.
However, it's much simpler to have the hash to spit out data set HASH and do the
counting in a separate step (which in this case takes but 0.05 sec, anyway).
And it is simpler because the hash object, unfortunately, is not (hopefully
just yet) endowed with the first.x/last.x functionality, so counting usi
ng the iterator would involve a good deal of fairly distasteful control-break
logic a la COBOL. Now compare it to the code based on hash of hashes:

data want_hoh (keep = nvar count) ;
  set have end = end ;
  array nn num: ;
  if _n_ = 1 then do ;
    dcl hash hh () ;
    hh.definekey ("_i_") ;
    hh.definedata ("h") ;
    hh.definedone () ;
    do over nn ;
      dcl hash h () ;
      h.definekey ("_n_") ;
      h.definedone () ;
      hh.add() ;
    end ;
  end ;
  do over nn ;
    hh.find() ;
    h.ref (key:nn, data:nn) ;
  end ;
  if end then do over nn ;
    Nvar = put (vname (nn), $8.) ;
    hh.find() ;
    Count = h.num_items ;
    output ;
  end ;
run ;

Interestingly, the "simple" single-hash code runs against HAVE as concocted
above (on my laptop) in 20 secs vs 28 secs for HOH, though I'd expected the
disparity to be quite a bit larger. But it should become more pronounced as W
grows and scanning through more hash instances gets more onerous.
The RAM footprints at W=1000 are also comparable (90 Mb for the single hash and 110 Mb f
or HOH).

Now note that while concocting the data, I added 1e6 to NN on purpose - to wit,
to make it out of reach for key-indexing (it would overflow the memory) and have
a chance to show that closed table array-based hashing with the simplest
collision resolution policy (i.e. linear probing), which (unlike key-indexing)
has no problem with any integer key range, stacks up against using the
hash object in both guises. (Granted, in the form shown below it is applicable
only to a simple integer key, while the hash object solutions can be used with
a character or composite key as well.)

* Load factor L defines hash table sparsity vs number of unique keys in it ;
* With L=0.5 about half of hash cells will remain unoccupied ;
* Step below finds the first prime number > U/L to size up hash array ;
* Same number is then used as modulo divisor in hash function mod(nn,&s) ;

%let L = 0.5 ;
data _null_ ;
  do p = ceil (&U / &L) by 1 until (j = up + 1) ;
    up = ceil (sqrt(p)) ;
    do j = 2 to up until (not mod(p,j)) ;
    end ;
  end ;
  call symputx ("S", p) ;
run ;

data want_hash_arr (keep = Nvar Count) ;
  array h [&w, &s] _temporary_ ;
  array q [&w, &s] _temporary_ ;
  set have end = end ;
  array nn num: ;
  do over nn ;
    do _n_ = 1 + mod (nn, &s) by 1 until (_h = nn or _h = .) ;
      if _n_ > &s then _n_ = 1 ;
      _h = h[_i_,_n_] ;
    end ;
    if _h ne . then continue ;
    h[_i_,_n_] = nn ;
    q[_i_,_n_] =  1 ;
  end ;
  if end ;
  call missing (of nn[*]) ;
  do over nn ;
    do _n_ = 1 to hbound (q, 2) ;
      nn ++ N (q[_i_,_n_]) ;
    end ;
    Nvar = put (vname (nn), $8.) ;
    Count = nn ;
    output ;
  end ;
run ;

Now, feel the difference: This guy runs in mere 3.5 seconds flat (5-6 times
the speed of either incarnation of the hash object code) and takes up 30 Mb
(1/3 the memory), even with the very sparse (and thus large) hash array table.

Of course, key-indexing still would run much faster - if the key range were
short enough for it to be usable (which I intentionally negated by adding
1e6 to the key). But it should be no surprise: The speed of this scheme
(identical for key-indexing and hash array) is defined by the tightness
of its inner loop - the first DO OVER NN - and the hash array code is much more involved t
han merely sticking 1 into the key-indexed cell whose number is NN.

Best regards
Paul Dofman




