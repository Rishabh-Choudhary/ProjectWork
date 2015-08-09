//Implemented by Rishabh Choudhary
#include<stdio.h>
#include<stdlib.h>
#include<math.h>

#define ALPHA     0.0001
#define BETA      0.95
#define UPPER     99.9
#define THRESHOLD 0.0001
#define LIMIT 2000


int  funcEval(int n, double *x, double *f_ptr)  {
/*** Objective function f = sum_i (x_i^2)/2                 ***
 ***                      + sum_i (a_i^2 * x_i^4)/16        ***
 ***                      + 0.5*(sum_i  b_i * xi)^2         ***/
   int i;
   double *a, *b, tmp1, tmp2, fval, qaz;

   a = (double *) calloc(n, sizeof(double));
   b = (double *) calloc(n, sizeof(double));
   if( (a==NULL) || (b==NULL) ) {
      printf("Memory allocation failed.\n");
      return 0; 
   }
   for(i=0; i<n; i++) {a[i] =2.0*i;  b[i] = 0.1*i;}

   qaz = b[0]*x[0];
   tmp1 = x[0]*x[0];   tmp2 = 0.25*a[0]*tmp1;
   fval = 0.5*tmp1 + tmp2*tmp2;
   for(i=1; i<n; i++) {
      qaz += b[i]*x[i];
      tmp1 = x[i]*x[i];   tmp2 = 0.25*a[i]*tmp1; 
      fval += 0.5*tmp1 + tmp2*tmp2;
   }
   *f_ptr = fval + 0.5*qaz*qaz;
   free(a);   
   free(b);
   return 1;

}

int  gradEval(int n, double *x, double *g)  {
   int i;
   double f1, f2, delta, tmp;

   delta = 0.00001;   
   tmp = 0.5/delta;
   for(i=0; i<n; i++) {
      x[i] = x[i] - delta ;
      funcEval(n, x, &f1);
      x[i] = x[i] + delta + delta;
      funcEval(n, x, &f2);
      x[i] = x[i] - delta;

      g[i] = (f2-f1)*tmp;
   }  
   return 1;
}



int calc_DirectionDerivatives(int n, double *dir, double *x, double f, 
                             double *deriv1_ptr,  double *deriv2_ptr)
{  int i;
   double *a, f1, f2, delta, tmp;

   a = (double *) calloc(n, sizeof(double));
   if( a == NULL ) {
      printf("Memory allocation failed.\n");
      return 0; 
   }
   delta= 0.00001;   
   tmp  = 1.0/delta;
   for(i=0; i<n; i++) a[i] = x[i] - delta*dir[i] ;
   funcEval(n, a, &f1) ;
   for(i=0; i<n; i++) a[i] = x[i] + delta*dir[i] ;
   funcEval(n, a, &f2);
   *deriv1_ptr = 0.5*(f2-f1)*tmp;
   *deriv2_ptr = (f2+f1-f-f)*tmp*tmp;

   free(a) ;
   return 1;
}


int line_search(int n, double *dir, double *x1, double f1, double *g1,
                                    double *x2, double *f2_ptr, double *g2)
{  int i, stop_LineSearch,iterator_limit=30;
   double step_size, wc1, wc2, lower, upper, f2, 
          deriv11, deriv12, deriv21, deriv22,delta ;

   delta= 0.00001;   
   lower = 0.0;
   upper = UPPER;
   calc_DirectionDerivatives(n, dir, x1, f1, &deriv11, &deriv12) ;
   if(deriv12>0.000000001) {
      step_size = lower-deriv11/(deriv12+0.0000000001);/*** Newton method ***/
   }
   else {
      step_size = 0.5*(lower+upper) ;
   }

   stop_LineSearch = 0 ;
   do {
      for(i=0; i<n; i++)
         x2[i] = x1[i] + step_size*dir[i] ;
      funcEval(n, x2, &f2);
      calc_DirectionDerivatives(n, dir, x2, f2, &deriv21, &deriv22) ;
      wc1 = f2-f1-ALPHA*step_size*deriv11;
      wc2 = abs(deriv21) - BETA*abs(deriv11);

	iterator_limit--;
      if( ((wc1<0.0) && (wc2<0.0)) || iterator_limit<0 ) { 
         stop_LineSearch = 1 ;
      }
      else {
         if(deriv21 > 0) { upper = step_size; }
         else            { lower = step_size; }

         if(deriv22>0.00000001) {
            step_size = step_size - deriv21/(deriv22+0.0000000001);
         }
         else {
            step_size = 0.5*(lower+upper) ;
         }
      }
   } while (stop_LineSearch == 0);
   gradEval(n, x2, g2);
   return 1;
}

int calc_norm(int n, double * g1, double * norm_g) 
{
	*norm_g=0;
	while(n>0)
	{
		n--;
		*norm_g+=(g1[n]*g1[n]);
	}
	*norm_g=sqrt(*norm_g);
	return 1;
}


int choose_SearchDirection(int n, double *x1,double * g1,double * dir) 
{
	double norm_g;
      	calc_norm(n, g1, &norm_g) ;
	while(n>0)
	{
		n--;
		dir[n]=(-1)*g1[n]/norm_g;
	}
	return 1;
}

int choose_initial_solution(int n,double * x1)
{
	int i;
	for(i=0;i<n;i++)
		x1[i]=pow((double)(-1),(i%2))*3;
	return 1;
}

int nonlinear_optimization(int n, double *x1, double *x2) {
   int iterat_num, i;
   double *x0, f1, f2, *g1, *g2, *dir,  norm_g ;

   x0 = (double *) calloc(n, sizeof(double));
   g1 = (double *) calloc(n, sizeof(double));
   g2 = (double *) calloc(n, sizeof(double));
   dir= (double *) calloc(n, sizeof(double));
   if( (x0==NULL)||(g1==NULL)||(g2==NULL)||(dir==NULL) ) {
      printf("Memory allocation failed.\n");
      return 0; 
   }

   choose_initial_solution(n, x1);
   funcEval(n, x1, &f1);
   gradEval(n, x1, g1) ;
   calc_norm(n, g1, &norm_g); 
   iterat_num = 0;
   while((norm_g > THRESHOLD)&&(iterat_num < LIMIT)) {
      iterat_num = iterat_num + 1 ;
      choose_SearchDirection(n, x1, g1, dir) ;
      line_search(n, dir, x1, f1, g1, x2, &f2, g2) ;
      
      f1 = f2 ;
      for(i=0; i<n; i++) {
         x1[i] = x2[i] ;
         g1[i] = g2[i] ;
      }
      calc_norm(n, g1, &norm_g) ;
   }
   return iterat_num;
}

int main()
{
int n=100;
double *x1,*x2,f1,*g1,norm_g;

x1=(double*)malloc(n*sizeof(double));
x2=(double*)malloc(n*sizeof(double));
g1=(double*)malloc(n*sizeof(double));

printf("Iteration number:%d\n",nonlinear_optimization(n, x1, x2));
funcEval(n, x1, &f1);
printf("FuncEval X1:%lf\n",f1);
gradEval(n, x1, g1) ;
calc_norm(n, g1, &norm_g); 
printf("Norm G1:%lf\n",norm_g);
return 1;
}
