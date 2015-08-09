//Implemented by Rishabh Choudhary
#include<stdio.h>
#include<stdlib.h>
#include<math.h>
#define DELTA 0.01 

int main()
{
	int i,n,err=0;
	double *x,*g,**H,*f; 
	int printDelta();
	int setVariables(int,double *);
	int printVariables(int ,double *);
	int funcEval(int,double *,double *);
	int printfuncEval(double);
	int gradEval(int,double *,double *);
	int printgradEval(int,double *);
	int HessEval(int,double *,double **);
	int printHessEval(int,double **);
        printf("\nNumber of variables : ");
	scanf("%d",&n); 
	H=malloc(n*sizeof(double *));
	if(H == NULL)
        {
                printf("out of memory\n");
                return 1;
        }
        for(i = 0; i < n; i++)
        {
                H[i] = malloc(n * sizeof(double));
                if(H[i] == NULL)
                {
                        printf("out of memory\n");
                        return 1;
                }
        }
        x=malloc(n*sizeof(double));
        if(x == NULL)
        {       
                printf("out of memory\n");
                return 1;
        }
        g=malloc(n*sizeof(double));
	if(g == NULL)
	{
		printf("out of memory\n");
		return 1;
	}
        f=malloc(sizeof(double));
	if(f == NULL)
	{
		printf("out of memory\n");
		return 1;
	}
	/*Accept values of variables in x*/
	err=setVariables(n,(double*)x);	
	err=printDelta();
	/*Evaluate funcEval*/
	err=funcEval(n,(double*)x,(double*)f);
	err=printVariables(n,(double*)x);
	err=printfuncEval(*f);
	/*Evaluate Gradient*/
	err=gradEval(n,(double*)x,(double*)g);
	err=printgradEval(n,(double*)g);
	/*Evaluate Hessian*/
	err=HessEval(n,(double*)x,(double**)H);
	err=printHessEval(n,(double**)H);
	return err;	
}

int printDelta()
{
	printf("\nDelta : %lf\n",DELTA);
	return 0;
}

int setVariables(int n,double *x)
{
	while(--n>=0)	
	{	
		printf("\nValue of x%d : ",n);
		scanf("%lf",&x[n]);
	}
	return 0;
}

int printVariables(int n,double *x)
{
	printf("\n\nVariables :\n");
	while(--n>=0)
		printf("\tx%d = %lf\n",n,x[n]);
	return 0;
}

int funcEval(int n,double *x,double *f)
{
	*f=0;
	/*f(x)=2^x[n-1]+2^x[n-2]+....+2^x[0]*/	
	while(--n>=0)
		*f+=pow((double)2,x[n]);
	return 0;	
}

int printfuncEval(double f)
{
	printf("\n\nValue of f : %lf\n",f);
	return 0;
}  

int gradEval(int n,double *x,double *g)
{
	int i,j,err=0;
	double x1[n],x2[n];
	/*g[i]=df/dx[i]=(f(x+DELTA)-f(x-DELTA))/(2*DELTA)*/
	/*f1 is f(x+DELTA) & f2 is f(x-DELTA)*/	
	double *f1,*f2;
        f1=malloc(sizeof(double));
	if(f1 == NULL)
	{
		printf("out of memory\n");
		return 1;
	}
        f2=malloc(sizeof(double));
	if(f2 == NULL)
	{
		printf("out of memory\n");
		return 1;
	}
	for(j=0;j<n;j++)
	{
		for(i=0;i<n;i++)
		{
			if(i==j)
			{
				/*Preparing x1 and x2 for f1 and f2 respectively. Change value of X[i] only and keep rest of the values in x same as original*/ 
				x1[i]=x[i]+DELTA;
				x2[i]=x[i]-DELTA;
			}
			else
			{
				x1[i]=x[i];
				x2[i]=x[i];
 			}
		}
		err=funcEval(n,(double*)x1,(double*)f1);
		err=funcEval(n,(double*)x2,(double*)f2);
		/*Evaluate Gradient g[i] as per the formula*/
		g[j]=(*f1-*f2)/(2*DELTA);
	}
	return err;
}

int printgradEval(int n,double *g)
{
	printf("\n\nGradient :\n");
	while(--n>=0)
		printf("\tg%d = %lf\n",n,g[n]);
	return 0;
}

int HessEval(int n,double *x,double **H)
{
	int i,j,k,err=0;
	double x1[n],x2[n],x3[n],x4[n];	
	/*df/dx[i]dx[j]=((f(x[i]+DELTA,x[j]+DELTA)-f(x[i]+DELTA,x[j]-DELTA))/(2*DELTA)-(f(x[i]-DELTA,x[j]+DELTA)-f(x[i]-DELTA,x[j]-DELTA))/(2*DELTA))/(2*DELTA)*/
	/*f1=f(x[i]+DELTA,x[j]+DELTA)*/
	/*f2=f(x[i]+DELTA,x[j]-DELTA)*/
	/*f3=f(x[i]-DELTA,x[j]+DELTA)*/
	/*f4=f(x[i]-DELTA,x[j]-DELTA)*/
	double *f1,*f2,*f3,*f4;
        f1=malloc(sizeof(double));
	if(f1 == NULL)
	{
		printf("out of memory\n");
		return 1;
	}
        f2=malloc(sizeof(double));
	if(f2 == NULL)
	{
		printf("out of memory\n");
		return 1;
	}
	f3=malloc(sizeof(double));
	if(f3 == NULL)
	{
		printf("out of memory\n");
		return 1;
	}
	f4=malloc(sizeof(double));
	if(f4 == NULL)
	{
		printf("out of memory\n");
		return 1;
	}	
	for(i=0;i<n;i++)
	{
		for(j=0;j<n;j++)
		{
			if(i!=j)
			{
				/*For elements other than diagonal elements of H*/
				/*df/dx[i]dx[j]=((f(x[i]+DELTA,x[j]+DELTA)-f(x[i]+DELTA,x[j]-DELTA))/(2*DELTA)-(f(x[i]-DELTA,x[j]+DELTA)-f(x[i]-DELTA,x[j]-DELTA))/(2*DELTA))/(2*DELTA)*/
				/*f1=f(x[i]+DELTA,x[j]+DELTA)*/
				/*f2=f(x[i]+DELTA,x[j]-DELTA)*/
				/*f3=f(x[i]-DELTA,x[j]+DELTA)*/
				/*f4=f(x[i]-DELTA,x[j]-DELTA)*/
				for(k=0;k<n;k++)
				{
					if(k==j)
					{
						/*Change value of x for diff w.r.t.o. x[j]*/
						x1[k]=x[k]+DELTA;
						x2[k]=x[k]-DELTA;
						x3[k]=x[k]+DELTA;
						x4[k]=x[k]-DELTA;
					}
					else if(k==i)
					{
						/*Change value of x for diff w.r.t.o. x[i]*/
						x1[k]=x[k]+DELTA;
						x2[k]=x[k]+DELTA;
						x3[k]=x[k]-DELTA;
						x4[k]=x[k]-DELTA;
					}
					else
					{
						/*Keep rest of the values same*/
						x1[k]=x[k];
						x2[k]=x[k];
						x3[k]=x[k];
						x4[k]=x[k];
 					}
				}
				/*Evaluate f1,f2,f3,f4*/
				err=funcEval(n,(double*)x1,(double*)f1);
				err=funcEval(n,(double*)x2,(double*)f2);
				err=funcEval(n,(double*)x3,(double*)f3);
				err=funcEval(n,(double*)x4,(double*)f4);
				/*Evaluate H[i][j]*/
				H[i][j]=((*f1-*f2)/(2*DELTA)-(*f3-*f4)/(2*DELTA))/(2*DELTA);
			}
			else
			{
				/*For diagonal elements*/
				/*df/dx[i]dx[i]=(f(x[i]+2*DELTA)-f(x[i])+f(x[i]-2*DELTA))/(4*DELTA*DELTA)*/
				/*f1=f(x[i]+2*DELTA)*/
				/*f2=f(x[i])*/
				/*f3=f(x[i]-2*DELTA)*/
				for(k=0;k<n;k++)
				{
					if(k==i)
					{
						/*Change value of x for diff w.r.t.o x[i][i]*/
						x1[k]=x[k]+(2*DELTA);
						x2[k]=x[k];
						x3[k]=x[k]-(2*DELTA);
					}
					else
					{		
						/*Keep rest of the values same*/
						x1[k]=x[k];
						x2[k]=x[k];
						x3[k]=x[k];
					}
				}
				/*Evaluate f1,f2,f3*/
				err=funcEval(n,(double*)x1,(double*)f1);
				err=funcEval(n,(double*)x2,(double*)f2);
				err=funcEval(n,(double*)x3,(double*)f3);
				/*Evaluate H[i][j]*/
				H[i][j]=(*f1-(2*(*f2))+*f3)/(4*DELTA*DELTA);
			}
		}
	}
	return err;
}

int printHessEval(int n,double **H)
{
	int i,j;	
	printf("\n\nHessian Matrix :\n");
	for(i=0;i<n;i++)
	{
		printf("\n|");
		for(j=0;j<n;j++)
		{
			printf("%10lf",H[i][j]);
		}
		printf("|");
	}
	printf("\n");
	return 0;
}	

