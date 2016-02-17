#include<mpi.h>
#include<math.h>
#define MIN(a,b) (((a)<(b))?(a):(b))
#define INF 9999
int floyd(int **W0, int n, MPI_Comm Comm)
{
	int myrank,size,i,j,k,rows_per_process;
	int **W,*Wk;
	MPI_Comm_rank(Comm,&myrank);
	MPI_Comm_size(Comm,&size);
	rows_per_process=ceil(((float)n)/size);	
	W=(int **)calloc(rows_per_process,sizeof(int*));
	Wk=(int *)calloc(n,sizeof(int));
	printf("Process %d :: Processing rows %d to %d\n",myrank,myrank*rows_per_process,((myrank+1)*rows_per_process)-1); 
	for(k=0;k<n;k++)
	{
		if(myrank==k/rows_per_process)
		{
			for(i=0;i<n;i++)
				Wk[i]=W0[k%rows_per_process][i];	
			printf("Process %d :: Broadcasting row %d to all other processes\n",myrank,k);
		}		
		MPI_Bcast (Wk, n, MPI_INT, k/rows_per_process, Comm);
		for(i=0;i<rows_per_process;i++)
		{
			int globali=i+(rows_per_process*myrank);
			printf("Process %d :: Evaluating row %d\n",myrank,globali); 
			W[i]=(int*)calloc(n,sizeof(int));	
			for(j=0;j<n;j++)
			{
				W[i][j]=MIN(W0[i][j],W0[i][k]+Wk[j]);
			}
		}
		for(i=0;i<rows_per_process;i++)
		{
			int globali=i+(rows_per_process*myrank);
			printf("Process %d :: Updating row %d of W0\n",myrank,globali); 
			for(j=0;j<n;j++)
			{
				W0[i][j]=W[i][j];
			}
		}
	}
	return 0;	
}

void initializeEdgeMatrix(int **edge, int size)
{
	int i,j;
	for(i=0; i<size; i++)
		edge[i] = (int *)calloc(size, sizeof(int));
	//initialize the Matrix as per the graph
	for(i=0;i<size-1;i++)
	{
		edge[i][i]=0;
		edge[i][i+1]=1;
		edge[i+1][i]=1;
		for(j=i+2;j<size;j++)
		{
			edge[i][j]=INF;
			edge[j][i]=INF;
		}		
	}	

}

int main(int argc,char ** argv)
{
	int myrank,i,j;
	int **Wfull,**W0,size=6,noProcess,*dist;
	Wfull = (int **)calloc(size,sizeof(int*));
	W0 = (int **)calloc(size,sizeof(int*));
	MPI_Init(&argc,&argv);
	MPI_Request request;
	MPI_Status status;
	MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
	MPI_Comm_size(MPI_COMM_WORLD,&noProcess);
	initializeEdgeMatrix(Wfull, size);	
	int rows_per_process=ceil(((float)size)/noProcess);	
	if(myrank==0)
	{
		printf("Initial Adjacancy Matrix:\n");	
		for(i=0;i<size;i++)
		{
			for(j=0;j<size;j++)
			{
				printf("%6d ",Wfull[i][j]);
			}
		printf("\n");
		}
		printf("\n");
	}
	for(i=0;i<rows_per_process;i++)
	{
		W0[i] = (int *)calloc(rows_per_process, sizeof(int));
		int globali=i+(rows_per_process*myrank);
		for(j=0;j<size;j++)
			W0[i][j]=Wfull[globali][j];
	}
	floyd(W0,size,MPI_COMM_WORLD);		
	for(i=0;i<rows_per_process;i++)
	{
		int globali=i+(rows_per_process*myrank);
		printf("Process %d :: Sending row %d to process 0\n",myrank,globali); 
		MPI_Isend(W0[i], size,MPI_INT,0,globali,MPI_COMM_WORLD,&request);
	}	
	if(myrank==0)
	{
		for(i=0;i<size;i++)
		{
			MPI_Irecv(Wfull[i],size,MPI_INT,i/rows_per_process,i,MPI_COMM_WORLD,&request);
			MPI_Wait(&request,&status);
			printf("Process %d :: Recieved row %d from process %d\n",myrank,i,i/rows_per_process); 
		}
	}
	MPI_Barrier(MPI_COMM_WORLD);	
	if(myrank==0)
	{
		printf("Adjacancy Matrix after Floyd's algorithm:\n");	
		for(i=0;i<size;i++)
		{
			for(j=0;j<size;j++)
			{
				printf("%6d ",Wfull[i][j]);
			}
		printf("\n");
		}
		printf("\n");
	}		
	MPI_Finalize();
	return 0;
}
