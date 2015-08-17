/*
 S0 -> Var | 'True' | 'False' | '(' S6 ')'

 S2 -> S0 | '~' S2

 S3 -> S2 | S3 '&' S2

 S4 -> S3 | S4 'or' S3

 S5 -> S4 | S4 '=>' S5

 S6 -> S5 | S5 '<=>' S6
*/

s0(L,T):-
L=['True'],T='True'
;
L=['False'],T='False'
;
append(['('|A],[')'],L),s6(A,T)
;
not(L=['True'];L=['False']),L=[H],T=H.

s2(L,T):-
s0(L,T);
L=['~'|A],s2(A,T1),T=['~',T1].

s3(L,T):-
s2(L,T);
append(A,['&'|B],L),s3(A,T1),s2(B,T2),T=['&',T1,T2].

s4(L,T):-
s3(L,T);
append(A,['or'|B],L),s4(A,T1),s3(B,T2),T=['or',T1,T2].

s5(L,T):-
s4(L,T);
append(A,['=>'|B],L),s4(A,T1),s5(B,T2),T=['=>',T1,T2].

s6(L,T):-
s5(L,T);
append(A,['<=>'|B],L),s5(A,T1),s6(B,T2),T=['<=>',T1,T2].

parse(L,T):-s6(L,T).

evaluate(T,V):-
T='True',V=true
;
T='False',V=false
;
T=['~',X],evaluate(X,Xval),(Xval=true,V=false;Xval=false,V=true)
;
T=['&',X,Y],evaluate(X,Xval),evaluate(Y,Yval),
	(Xval=true,Yval=true,V=true;
	Xval=false,Yval=true,V=false;
	Xval=true,Yval=false,V=false;
	Xval=false,Yval=false,V=false)
;
T=['or',X,Y],evaluate(X,Xval),evaluate(Y,Yval),
	(Xval=false,Yval=false,V=false;
	Xval=true,Yval=false,V=true;
	Xval=false,Yval=true,V=true;
	Xval=true,Yval=true,V=true)
;
T=['=>',X,Y],evaluate(X,Xval),evaluate(Y,Yval),
	(Xval=true,Yval=false,V=false;
	Xval=false,Yval=true,V=true;
	Xval=false,Yval=false,V=true;
	Xval=true,Yval=true,V=true)
;
T=['<=>',X,Y],evaluate(X,Xval),evaluate(Y,Yval),
	(Xval=false,Yval=false,V=true;
	Xval=true,Yval=true,V=true;
	Xval=true,Yval=false,V=false;
	Xval=false,Yval=true,V=false).


findVar(S,V):-
not(S=[_|_]),((S='False';S='True'),V=[];not(S='False';S='True'),V=[S])
;
S=[_,T],findVar(T,V)
;
S=[_,T1,T2],findVar(T1,V1),findVar(T2,V2),append(V1,V2,V3),sort(V3,V).

crossProd(X,Y,R):-
  X=[], R=[]
  ;
  X=[H|XT],
  crossProd(XT,Y,R1),
  singletonCP(H,Y,R2),
  append(R1,R2,R).


singletonCP(X,Y,R):-
  Y=[],R=[] ;
  Y=[H|T], singletonCP(X,T,R1), R=[[X,H]|R1].

flatten(X,Y):-
	X=[],Y=[]
	;
	X=[H|T],
	flatten(H,Y2),
	flatten(T,Y3),
	append(Y2,Y3,Y)
	;
	not(X=[_|_];X=[]),Y=[X].

/*cross product for truth values for 3 variables p,q,r gives truthValues as
X=[[[T,F],T],[[T,F],F],[[T,T],T],[[T,T],F],...]
flattenEachElement(X,Y) takes X and gives Y as
Y=[[T,F,T],[T,F,F],[T,T,T],[T,T,F],...]
*/

flattenEachElement(X,Y):-
X=[],Y=[]
;
X=[H|T],
flattenEachElement(T,T1),
flatten(H,H1),
Y=[H1|T1].

truthValues(V,TV):-
V=[_],TV=['True','False']
;
V=[_|T],
truthValues(T,TV1),
crossProd(TV1,['True','False'],TV).

truthValueList(V,TVL):-
truthValues(V,TV),flattenEachElement(TV,TVL).

/*replace element P in list X with V and create list Y*/
replaceElementInList(X,P,V,Y):-
X=[],Y=[]
;
X=[H|T],
replaceElementInList(T,P,V,Y1),
(H=P,Y=[V|Y1];not(H=P;H=[_|_]),Y=[H|Y1];H=[_|_],replaceElementInList(H,P,V,H1),Y=[H1|Y1]).

/*replace each element with is corresponding truth value*/
replaceElement(X,P,V,Y):-
X=[],Y=[]
;
P=[],V=[],Y=X
;
V=[V1|V2],
P=[P1|P2],
replaceElementInList(X,P1,V1,X1),
replaceElement(X1,P2,V2,Y).

/*traverse through the truth value list and substitute each set of truth values in the S Expression*/
substituteValues(TVL,V,S,R):-
S=[],R=[]
;
TVL=[],R=[]
;
TVL=[TVL1|TVL2],
replaceElement(S,V,TVL1,S1),
substituteValues(TVL2,V,S,R1),
R=[S1|R1].

/*Check if the S-expression is a tautology*/
checkTautology(R):-
R=[]
;
R=[H|T],
evaluate(H,V),
V=true,
checkTautology(T).

taut(E):-
parse(E,S),findVar(S,V),truthValueList(V,TVL),
substituteValues(TVL,V,S,R),checkTautology(R).


























