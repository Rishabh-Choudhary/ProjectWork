//Implemented by Rishabh Choudhary
#include<stdio.h>
#include<stdlib.h>

int main()
{
	int i,err,n;
	double **A,*x,*b;
	/*Get the dimension of matrix into n*/ 
	printf("\nEnter the size of the matrix : ");
	scanf("%d",&n);
	/*Allocate memory for A,x,b*/	
	A=malloc(n*sizeof(double *));
	if(A == NULL)
		{
		printf("out of memory\n");
		return 1;
		}
	for(i = 0; i < n; i++)
		{
		A[i] = malloc(n * sizeof(double));
		if(A[i] == NULL)
			{
			printf("out of memory\n");
			return 1;
			}
		}
	b=malloc(n*sizeof(double));
	if(b == NULL)
		{
		printf("out of memory\n");
		return 1;
		}
	x=malloc(n*sizeof(double));
	if(x == NULL)
		{
		printf("out of memory\n");
		return 1;
		}
	/*Initialize all matrices to 0*/	
	err=InitializeMatrices(n,(double**)A,(double*)b,(double*)x);
	/*Accept values for elements of matrix*/
	err=SetValuesInMatrices(n,(double**)A,(double*)b,(double*)x);	
	/*Print matrix with initially set values*/	
	err=PrintMatrice(n,(double**)A,(double*)b,(double*)x);
	/*Evaluate values of x by Gauss Elimination*/
	err=GaussElim(n,(double**)A,(double*)b,(double*)x);
	/*Print matrix after evaluation process*/
	err=PrintMatrice(n,(double**)A,(double*)b,(double*)x);
	return err;
}

int InitializeMatrices(int n,double **A,double *b,double *x)
{
	int row,col,err=0;
	for(row=0;row<n;row++)
	{
		for(col=0;col<n;col++)
			A[row][col]=0;
		x[row]=0;
		b[row]=0;
	}
	return err;
}

int SetValuesInMatrices(int n,double **A,double *b,double *x)
{
	int i,j,err=0;
	for(i=0;i<n;i++)
	{	
		for(j=0;j<n;j++)
		{
			system("clear");
			/*Display matrices with already filled in values*/
			err=PrintMatrice(n,(double**)A,(double*)b,(double*)x);
			/*Get value for i,j position of matrix A*/
			printf("\nValue for A[%d][%d] : ",i,j);	
			scanf("%lf",&A[i][j]);
		}
	}
 	for(i=0;i<n;i++)
	{
		system("clear");
		/*Display matrices with already filled in values*/
		err=PrintMatrice(n,(double**)A,(double*)b,(double*)x);
		/*Get value for i row of matrix b*/
		printf("\nValue for b[%d] : ",i);	
		scanf("%lf",&b[i]);	
	}
	system("clear");
	return err;
}
 
int GaussElim(int n, double **A, double *b, double *x)
{
	int row,col,tmpcol,err=0;
	double factor1,factor2;
	/*Process 1 column at a time*/
	for(col=0;col<n;col++)
	{
		for(row=n-1;row>col;row--)
		{
			/*For each row n mutiply each value by the value at n-1,col and multiply each value in row n-1 by value at n,col.Then subtract each value of row n by correspondng value in row n-1 to make n,col 0*/
			factor1=A[row-1][col];
			factor2=A[row][col];
			for(tmpcol=col;tmpcol<n;tmpcol++)
			{
				A[row-1][tmpcol]=A[row-1][tmpcol]*factor2;
				A[row][tmpcol]=(A[row][tmpcol]*factor1)-A[row-1][tmpcol];
			}
			/* Do same as done for matrix A to matrix b*/
			b[row-1]=b[row-1]*factor2;
			b[row]=(b[row]*factor1)-b[row-1];
			/*Print steps*/
			printf("R%d -> R%d * %lf\n",row-1,row-1,factor2);	
			printf("R%d -> ( R%d * %lf ) - R%d\n",row,row,factor1,row-1);	
			/*Print matrix after completion of step*/
			err=PrintMatrice(n,(double**)A,(double*)b,(double*)x);
		}
		/*Make diagonal element in the column 1 by dividing entire row of the diagonal element by the value of the diagonal element*/
		factor1=A[col][col];
		for(tmpcol=col;tmpcol<n;tmpcol++)
		{
			if(A[col][tmpcol]) A[col][tmpcol]=A[col][tmpcol]/factor1;
		}
		/*Do the same for matrix b*/
		if(b[col]) b[col]=b[col]/factor1;	
		/*Print step*/ 
		printf("R%d -> ( R%d / %lf )\n",col,col,factor1);
		/*Print matrix after completion of step*/
		err=PrintMatrice(n,(double**)A,(double*)b,(double*)x);
	}
	/*Evaluate values of x by backtracking*/
	x[n-1]=b[n-1];
	printf("x%d = b%d = %lf\n",n-1,n-1,b[n-1]);	
	for(row=n-2;row>=0;row--)
	{
		x[row]=b[row];
		printf("x%d = b%d",row,row);	
		for(col=n-1;col>row;col--)
		{
			x[row]=x[row]-A[row][col]*x[col];
			printf(" - A%d%d * x%d",row,col,col);
		}
		printf(" = %lf\n",x[row]);
	}
	return err;
}

int PrintMatrice(int n,double **A,double *b,double *x)
{
	/*Print values of matrix A,x and b*/
	int i,j;
	printf("\n");	
	for(i=0;i<n;i++)
	{
		printf("|");
		for(j=0;j<n;j++)
		{	
			printf("%10lf",A[i][j]);
		}
		printf("|\t|%10lf|",x[i]);
		if(i==n/2) printf("\t=");
		 else printf("\t ");
		printf("\t|%10lf|\n",b[i]);
	}
	for(i=0;i<n+2;i++)
	{
		if(i==n/2) printf("A");
		else if(i==n) printf("\t\tx\t");
		else if(i==n+1) printf("\t\tb\n");
		else printf("\t\t");
	}
	printf("\n");	
	return 0;
}

