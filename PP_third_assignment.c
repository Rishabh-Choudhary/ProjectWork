#include <stdio.h>
#include<mpi.h>
#include <stdlib.h>
#include <math.h>
#define INF 9999
#define MIN(X,Y) ((X) < (Y) ? (X) : (Y))

int chooseVertex(int *dist, int n, int *found, int noProcess, MPI_Comm Comm)
{
	int i, *tmp, least = INF, leastPosition, *tmpleastPosition;
	MPI_Request request;
	MPI_Status status;
	int perProcess = ceil(((float)n) / noProcess);
	tmp=(int*)calloc(1,sizeof(int));	
	tmpleastPosition =(int*)calloc(1,sizeof(int));	
	for(i=1; i<noProcess;i++)
	{
		int k;
		int *tempDist = (int*)calloc(perProcess,sizeof(int));
		int *tempFound = (int*)calloc(perProcess,sizeof(int));
		int elementCount=0;	
		for(k=0; k<perProcess; k++)
		{      
			if((i*perProcess+k)<n)	
			{
				tempDist[k] = dist[i*perProcess+k]; 
				tempFound[k] = found[i*perProcess+k];
				elementCount++;
			}
		}
		MPI_Isend(&elementCount, 1,MPI_INT,i,10,Comm,&request);
		MPI_Isend(tempDist, elementCount,MPI_INT,i,1,Comm,&request);
		MPI_Isend(tempFound,elementCount,MPI_INT,i,2,Comm,&request);
	}
	//task
	for(i=0;i<perProcess;i++)
	{
		*tmp = dist[i];
		if( (!found[i]) && ((*tmp) < least))
		{
			least = (*tmp);
			leastPosition = i;
		}
	}
	for(i=1;i<noProcess;i++)
	{
		MPI_Irecv(tmp,1,MPI_INT,i,3,Comm,&request);
		MPI_Wait(&request,&status);
		MPI_Irecv(tmpleastPosition,1,MPI_INT,i,4,Comm,&request);
		MPI_Wait(&request,&status);
		if( (*tmp) <= least)
		{
			least = (*tmp);
			leastPosition = (i*perProcess)+(*tmpleastPosition);
		}		
	}
return leastPosition;
}

void dijkstra(int SOURCE, int n, int **edge, int *dist, int myrank, int noProcess, MPI_Comm Comm) 
{
	if(myrank == 0)
	{
		int i,j,count,*found;
		MPI_Request request;
		MPI_Status status;
		int perProcess = ceil(((float)n) / noProcess);
		found = (int *)calloc(n,sizeof(int));
		for(i=0; i<n; i++)
		{
			found[i] = 0;
			dist[i] = edge[SOURCE][i];
		}
		found[SOURCE] = 1;
		count = 1;
		while(count<n)
		{
			j = chooseVertex(dist, n, found, noProcess, Comm);
			found[j] = 1;
			count++;
			
			for(i=1; i<noProcess; i++)
			{
			 int k;
			 int *tempDist = (int*)calloc(perProcess,sizeof(int));
			 int *tempFound = (int*)calloc(perProcess,sizeof(int));	
			 int elementCount=0; 
			 for(k=0; k<perProcess; k++)
			 {      	
				if((i*perProcess+k)<n)	
				{
					tempDist[k] = dist[i*perProcess+k]; 
					tempFound[k] = found[i*perProcess+k];
					elementCount++;
				}
			 }	
			 MPI_Isend(&elementCount,1,MPI_INT,i,11,Comm,&request);
			 MPI_Isend(tempDist, elementCount,MPI_INT,i,5,Comm,&request);
			 MPI_Isend(tempFound, elementCount,MPI_INT,i,6,Comm,&request);
			 MPI_Isend(edge[j],n,MPI_INT,i,7,Comm,&request);
			 MPI_Isend(&dist[j],1,MPI_INT,i,8,Comm,&request);
			}
			 //task 
			for(i=0; i<perProcess; i++)
			{
				if( !(found[i]))
					dist[i] = MIN(dist[i], dist[j]+edge[j][i]);	
			}	  
			for(i=1; i<noProcess;i++ )
			{
				int k;
				int *tempDist = (int*)calloc(perProcess,sizeof(int));
					 
				MPI_Irecv(tempDist, perProcess,MPI_INT,i,9,Comm,&request);
				MPI_Wait(&request,&status);
				for(k=0; k<perProcess; k++)
				{
					if((i*perProcess+k)<n)
						dist[i*perProcess+k] = tempDist[k];
				}

			}

		}//end while
	}//end if
	else
	{
		int count=1;
		int perProcess = ceil(((float)n) / noProcess);
		MPI_Request request;
		MPI_Status status;
		while(count<n)
	    {
			int *distj=(int*)calloc(1,sizeof(int)) ;
			int *elementCount=(int*)calloc(1,sizeof(int)) ;
			int i,tmp, least = INF, leastPosition;
			int *tempDist = (int*)calloc(perProcess,sizeof(int));
			int *tempFound = (int*)calloc(perProcess,sizeof(int));	
			int *edge = (int*)calloc(n,sizeof(int));	
			count++;
			MPI_Irecv(elementCount, 1,MPI_INT,0,10,Comm,&request);
			MPI_Wait(&request,&status);
			MPI_Irecv(tempDist, *elementCount,MPI_INT,0,1,Comm,&request);
			MPI_Wait(&request,&status);
			MPI_Irecv(tempFound,*elementCount,MPI_INT,0,2,Comm,&request);
			MPI_Wait(&request,&status);
			//choose vertex task
			for(i=0;i<*elementCount;i++)
			{
				tmp = tempDist[i];
				if( (!tempFound[i]) && (tmp < least))
				{
					least = tmp;
					leastPosition = i;
				}
			}
			MPI_Isend(&least,1,MPI_INT,0,3,Comm,&request);
			MPI_Isend(&leastPosition,1,MPI_INT,0,4,Comm,&request);
			MPI_Irecv(elementCount, 1,MPI_INT,0,11,Comm,&request);
			MPI_Wait(&request,&status);
			MPI_Irecv(tempDist, *elementCount,MPI_INT,0,5,Comm,&request);
			MPI_Wait(&request,&status);
			MPI_Irecv(tempFound, *elementCount,MPI_INT,0,6,Comm,&request);
			MPI_Wait(&request,&status);
			MPI_Irecv(edge,n,MPI_INT,0,7,Comm,&request);
			MPI_Wait(&request,&status);
			MPI_Irecv(distj,1,MPI_INT,0,8,Comm,&request);
			MPI_Wait(&request,&status);
			//task
			for(i=0; i<*elementCount; i++)
			{
				if( !(tempFound[i]))
					tempDist[i] = MIN(tempDist[i], (*distj)+edge[myrank*perProcess + i]);	
			}
			MPI_Isend(tempDist, perProcess,MPI_INT,0,9,Comm,&request);
		}
	}
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
	int **edge,size=6,noProcess,*dist;
	dist = (int*)calloc(size,sizeof(int));
	edge = (int **)calloc(size,sizeof(int*));
	MPI_Init(&argc,&argv);
	MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
	MPI_Comm_size(MPI_COMM_WORLD,&noProcess);
	if(myrank==0)
	{
		printf("\nInitial Dist: [");	
		for(i=0;i<size;i++)
		{
			printf("%d ",dist[i]);
		}
		printf("]\n");
		initializeEdgeMatrix(edge, size);	
		printf("\nEdge Matrix: \n");	
		for(i=0;i<size;i++)
		{
			for(j=0;j<size;j++)
			{
				printf("%6d ",edge[i][j]);
			}
		printf("\n");
		}
		printf("\n");
	}		
	dijkstra(0,size,edge,dist,myrank,noProcess,MPI_COMM_WORLD);
	if(myrank==0)
	{	
		printf("\nResultant Dist: [");	
		for(i=0;i<size;i++)
		{
			printf("%d ",dist[i]);
		}
		printf("]\n");
	}
	MPI_Finalize();
	return 0;
}


