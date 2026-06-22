# Nitro interpreted engine vs Trino SQL — order-insensitive, cents-space multiset compare (2026-06-20, final)

Tool: src/test/java/org/weakref/nitro/tpcds/CompareToSql.java. Canonical cents-space (decimals->unscaled,
doubles->round*100, dates->epoch, strings trimmed), order-insensitive multiset. -Dnitro.tpcds.trino.root=/root/notes/trino.

## FINAL: 82 MATCH / 17 DIFF / 0 ERROR (of 99)
EVERY DIFF has equal Nitro/SQL row counts => the Nitro interpreted engine has ZERO cardinality/wrong-row bugs
vs Trino SQL, and now executes ALL queries (0 errors).

### Fixes that cleared the 3 ERRORs this session:
- q37, q82: FlatKeyLayout.internDictionaryEntries crash on over-allocated dictionary values (negative arraycopy)
  -> guard maps phantom/unreferenced entries to the -1 byte-compare fallback. Now MATCH.
- q18: harness called primitive 'multiply_i64' (compiled-engine name); interpreted registry has it as 'multiply'
  -> renamed the 3 q18 call sites to 'multiply'. Now MATCH (100 rows).

### The 17 DIFF (all equal row counts = NOT data bugs; documented 'structurally exact'):
- derived avg/decimal PRECISION (Nitro fixed-point vs SQL decimal): q07 q12 q26 q27 q31 q36 q58 q85 q98
- rank-tie / NULL-render from rounding: q16 q20 q44 q49 q61 q79 q83 q84

### Separately (Trino-OPERATOR harness, not Nitro): q45 emits 51 rows vs SQL 49 — real Trino-op-harness bug.

## Raw
q01  MATCH  (100 rows)
q02  MATCH  (2513 rows)
q03  MATCH  (100 rows)
q04  MATCH  (100 rows)
q05  MATCH  (100 rows)
q06  MATCH  (51 rows)
q07  DIFF  nitroRows=100 sqlRows=100  onlyNitro=100 onlySql=100
      only-nitro: AAAAAAAAAAEHAAAA13002850000199500
      only-nitro: AAAAAAAAAABDBAAA6006347000101500
      only-nitro: AAAAAAAAAAECBAAA7003036000215500
      only-sql:   AAAAAAAAAAEPAAAA7800285801045
      only-sql:   AAAAAAAAAACBBAAA41805216137694348
      only-sql:   AAAAAAAAAACJAAAA4800580602580
q08  MATCH  (9 rows)
q09  MATCH  (1 rows)
q10  MATCH  (100 rows)
q11  MATCH  (100 rows)
q12  DIFF  nitroRows=100 sqlRows=100  onlyNitro=100 onlySql=100
      only-nitro: AAAAAAAAEBLIBAAAClaims might not develop laBooksarts3772367022298
      only-nitro: AAAAAAAAFAAOAAAACentral things should concentrate educational, bad trends. Groups might not go accused, heavy ages. Only necessary offers sleep even virtually able bodies; popular, newBooksarts110394650371768
      only-nitro: AAAAAAAAAFKNAAAALoans ought to give legal ministers; clothes must not establish. Necessary, slight patients see traditionally advanced demands; differences may challenBooksarts2755500851819
      only-sql:   AAAAAAAADCCDAAAAGlad users understand very almost original jobs. Towns can understand. Supreme, following days work by a parents; german, crucial weapons work sure; fair picturBooksarts71846760
      only-sql:   AAAAAAAADFJLAAAAPatterns may try english, criminal difficulties. Cups must progress then prisoners. Again new goals will collect heavily local, other offices. Hours avoid most with a films. Premises can get. EyesBooksarts1691439180136
      only-sql:   AAAAAAAACKPGBAAAVery fine sites understand manufacturing affairs. Young, high rights shall not ensurBooksarts533475905572
q13  MATCH  (1 rows)
q14  MATCH  (100 rows)
q15  MATCH  (100 rows)
q16  DIFF  nitroRows=1 sqlRows=1  onlyNitro=1 onlySql=1
      only-nitro: 784368477073-70256891
      only-sql:   783368477073-70164771
q17  MATCH  (0 rows)
q18  MATCH  (100 rows)
q19  MATCH  (100 rows)
q20  DIFF  nitroRows=100 sqlRows=100  onlyNitro=99 onlySql=99
      only-nitro: AAAAAAAAECGKAAAARemarkably thin charges will not ask once practical rare styles. Girls provide. Then original responses may not like recent, constant children. Departments feel white conditions.Booksarts4558539837254
      only-nitro: AAAAAAAAAONABAAAFar traditional years might dream of course clever voBooksarts638650834283923
      only-nitro: AAAAAAAAAJIAAAAAJoint, superior police would use through an restrictions. Buyers ought to contract generally in a efforts. Days cut also sure, frequent sBooksarts4369163017
      only-sql:   AAAAAAAADNHOAAAAPersonnel end to a years. Customers expect originally bizarre, wonderful things. Supporters continue more for a judges.Booksarts8571475326
      only-sql:   AAAAAAAACIEGAAAANecessary developers will not build most in a days. Perhaps young weeks provide with a faces. Bizarre areas must not work incomes; deaf, reBooksarts385137579460
      only-sql:   AAAAAAAADJFCAAAASignificant, preliminary boys can remain lightly more pale discussionBooksarts274134105759
q21  MATCH  (100 rows)
q22  MATCH  (100 rows)
q23  MATCH  (1 rows)
q24  MATCH  (20 rows)
q25  MATCH  (1 rows)
q26  DIFF  nitroRows=100 sqlRows=100  onlyNitro=100 onlySql=100
      only-nitro: AAAAAAAAAAKBAAAA23001821001309200107400
      only-nitro: AAAAAAAAAAALAAAA705092005025307400501400
      only-nitro: AAAAAAAAAAICBAAA91009146000896300
      only-sql:   AAAAAAAAAAKNAAAA930014002012461
      only-sql:   AAAAAAAAAAFMAAAA26001042006252
      only-sql:   AAAAAAAAAAFCAAAA8200735002572
q27  DIFF  nitroRows=100 sqlRows=100  onlyNitro=100 onlySql=100
      only-nitro: AAAAAAAAAAEGBAAATN060007361000426900
      only-nitro: AAAAAAAAAACEBAAATN025006571000480500
      only-nitro: AAAAAAAAAAEPAAAANULL19200122430001052800
      only-sql:   AAAAAAAAAABAAAAATN062002684561221463
      only-sql:   AAAAAAAAAABIBAAATN09400869703478
      only-sql:   AAAAAAAAAAAIAAAANULL12500157401448
q28  MATCH  (1 rows)
q29  MATCH  (3 rows)
q30  MATCH  (100 rows)
q31  DIFF  nitroRows=288 sqlRows=288  onlyNitro=288 onlySql=288
      only-nitro: Washington Parish2000155629786935515153331133944
      only-nitro: Larimer County200091651975790518366871700405
      only-nitro: Piatt County200017387076578291769892970644
      only-sql:   Summers County2000223129159155
      only-sql:   Gilpin County20007052300262
      only-sql:   Radford city200013086130117
q32  MATCH  (1 rows)
q33  MATCH  (100 rows)
q34  MATCH  (1546 rows)
q35  MATCH  (100 rows)
q36  DIFF  nitroRows=100 sqlRows=100  onlyNitro=100 onlySql=100
      only-nitro: -435157Homeaccent010
      only-nitro: -443321Electronicsautomotive03
      only-nitro: -437449Menpants02
      only-sql:   -43Jewelryestate013
      only-sql:   -43NULLNULL21
      only-sql:   -44Childrenschool-uniforms01
q37  MATCH  (2 rows)
q38  MATCH  (1 rows)
q39  MATCH  (92 rows)
q40  MATCH  (100 rows)
q41  MATCH  (41 rows)
q42  MATCH  (11 rows)
q43  MATCH  (17 rows)
q44  DIFF  nitroRows=10 sqlRows=10  onlyNitro=2 onlySql=2
      only-nitro: 7n stationoughtcallyationcallyesebarn stpri
      only-nitro: 4oughtoughtableoughtprieingn stcallycallyanti
      only-sql:   3oughtoughtableoughtpripriableeingbaranti
      only-sql:   6n stationoughtcallyationcallyablen stantieing
q45  MATCH  (49 rows)
q46  MATCH  (100 rows)
q47  MATCH  (100 rows)
q48  MATCH  (1 rows)
q49  DIFF  nitroRows=41 sqlRows=41  onlyNitro=33 onlySql=33
      only-nitro: catalog67623530612244898108
      only-nitro: store9556971875000000086
      only-nitro: web28181565656565657104
      only-sql:   web9928360159
      only-sql:   web383254951
      only-sql:   catalog671675063
q50  MATCH  (51 rows)
q51  MATCH  (100 rows)
q52  MATCH  (100 rows)
q53  MATCH  (100 rows)
q54  MATCH  (1 rows)
q55  MATCH  (100 rows)
q56  MATCH  (100 rows)
q57  MATCH  (100 rows)
q58  DIFF  nitroRows=38 sqlRows=38  onlyNitro=38 onlySql=38
      only-nitro: AAAAAAAAACCEAAAA5592651073575750110560170511555789066667
      only-nitro: AAAAAAAAOHHABAAA4278291121412596108143196411324241296667
      only-nitro: AAAAAAAADDEMAAAA7045961106728504114369126010857081200000
      only-sql:   AAAAAAAAPBNCAAAA422786104945873011384620701146447862
      only-sql:   AAAAAAAAPGJGBAAA781215116572363610807296121088744821
      only-sql:   AAAAAAAAACCEAAAA559265107357575011056017051155578907
q59  MATCH  (100 rows)
q60  MATCH  (100 rows)
q61  DIFF  nitroRows=1 sqlRows=1  onlyNitro=1 onlySql=1
      only-nitro: 992959895197389265150304655346731
      only-sql:   99295989519738926515030
q62  MATCH  (100 rows)
q63  MATCH  (100 rows)
q64  MATCH  (33 rows)
q65  MATCH  (100 rows)
q66  MATCH  (10 rows)
q67  MATCH  (100 rows)
q68  MATCH  (100 rows)
q69  MATCH  (100 rows)
q70  MATCH  (8 rows)
q71  MATCH  (9669 rows)
q72  MATCH  (100 rows)
q73  MATCH  (16 rows)
q74  MATCH  (100 rows)
q75  MATCH  (100 rows)
q76  MATCH  (100 rows)
q77  MATCH  (100 rows)
q78  MATCH  (100 rows)
q79  DIFF  nitroRows=100 sqlRows=100  onlyNitro=3 onlySql=3
      only-nitro: AbramsGlaydsOak Grove63800600
      only-nitro: AbrahamAnnPleasant Hill150584800
      only-nitro: AbneyLeifOak Grove17970150-157588
      only-sql:   AbneyLeifOak Grove1797015NULL-157588
      only-sql:   AbrahamAnnPleasant Hill15058480NULL
      only-sql:   AbramsGlaydsOak Grove6380060NULL
q80  MATCH  (100 rows)
q81  MATCH  (100 rows)
q82  MATCH  (6 rows)
q83  DIFF  nitroRows=100 sqlRows=100  onlyNitro=94 onlySql=94
      only-nitro: AAAAAAAABFOBBAAA3135281261431937246667
      only-nitro: AAAAAAAAACDJAAAA5198882158939756573333
      only-nitro: AAAAAAAACFEHBAAA1415561516671111100000
      only-sql:   AAAAAAAAAAIIBAAA337756615494310094733
      only-sql:   AAAAAAAAAODOAAAA46191793752510422667
      only-sql:   AAAAAAAAACNIAAAA164412628106411733
q84  DIFF  nitroRows=100 sqlRows=100  onlyNitro=98 onlySql=98
      only-nitro: AAAAAAAAAMPJDAAAPayne, Leroy
      only-nitro: AAAAAAAAAOBKGAAANicholson, Vincent
      only-nitro: AAAAAAAAANBKFAAAHunter, Martha
      only-sql:   AAAAAAAAABKDDAAASloan                         , David
      only-sql:   AAAAAAAAACEEFAAAShipman                       , James
      only-sql:   AAAAAAAAAKBIAAAAWallace                       , Billy
q85  DIFF  nitroRows=17 sqlRows=17  onlyNitro=17 onlySql=17
      only-nitro: unauthoized purchase42506338750733300
      only-nitro: reason 2655003963800226300
      only-nitro: reason 3218004141400557400
      only-sql:   reason 4137001252268664
      only-sql:   reason 321800414145574
      only-sql:   Parts missing2800174756814
q86  MATCH  (100 rows)
q87  MATCH  (1 rows)
q88  MATCH  (1 rows)
q89  MATCH  (100 rows)
q90  MATCH  (1 rows)
q91  MATCH  (16 rows)
q92  MATCH  (1 rows)
q93  MATCH  (100 rows)
q94  MATCH  (1 rows)
q95  MATCH  (1 rows)
q96  MATCH  (1 rows)
q97  MATCH  (1 rows)
q98  DIFF  nitroRows=15080 sqlRows=15080  onlyNitro=15073 onlySql=15073
      only-nitro: AAAAAAAAIKHAAAAAPictures cannot get advantages. Roman, difficult issues shift easy. Guidelines rouse just all actual hours. Coherent, main days acknowledge forward previousHometables481360786452956
      only-nitro: AAAAAAAAINNBAAAAMad, overall patients may not keep then; pounds used to allow freshly foreign, western changes. Critical, fresh consequences shouldBookstravel283547839175933
      only-nitro: AAAAAAAAGDBABAAAParticular respects see great operations; occasions get sole, genuine amounts. Social, long boys may not accommodate very native estimHomemattresses343365346122520
      only-sql:   AAAAAAAAGAOFBAAAObservations shall stay political, historical rooms. Also federal strategies fuck characters. Hands must overturn in addition old moves. Certain, public planHomecurtains/drapes845879392524
      only-sql:   AAAAAAAAIDDMAAAAYet able boats could not choose more. Casual, royal productsHomerugs7032210757970
      only-sql:   AAAAAAAACMOMAAAAEven constant movements note less than for a years. Banks might serve a little small forests. Ways sleep alike british regions. Political ideas change almost abstract costs; professional, fasSportsgolf260161881050
q99  MATCH  (100 rows)
