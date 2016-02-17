#define ROWS 1200	
#define COLS 1024
#include<mpi.h>

void computeAverage(int myrank,float *input,int rownum,int size,float *output)
{
	int i;
			for(i=0;i<COLS;i++)
			{
				*(output+rownum)+=input[i];
			}
			printf("\n%d :: Avg of row %d: %f",myrank,rownum+((int)myrank*ROWS/size),*(output+rownum)/COLS);
}

int main(argc,argv)
int argc;
char **argv;
{
 float **input,*output;
 int i,j,myrank,size;
 MPI_Status status;
 MPI_Request request;
 MPI_Init(&argc,&argv);
 MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
 MPI_Comm_size(MPI_COMM_WORLD,&size);
 input=(float**)malloc(ROWS*sizeof(float*));
 if(myrank==0)
 {
  for(i=0;i<ROWS;i++)
  {
	input[i]=(float*)malloc(COLS*sizeof(float));
  	for(j=0;j<COLS;j++)
	{	
		input[i][j]=(i+1);
	}
	MPI_Isend(input[i],COLS,MPI_FLOAT,(int)i/((int)ROWS/size),(int)i%((int)ROWS/size),MPI_COMM_WORLD,&request);
	printf("\n%d :: Row %d sent to %d with msg tag %d",myrank,i,(int)i/((int)ROWS/size),(int)i%((int)ROWS/size));
  }	
 }
 j=0;
 output=(float*)calloc(((int)ROWS/size),sizeof(float));
 while(j<(int)ROWS/size)
 { 
		input[j]=(float*)malloc(COLS*sizeof(float));
        	MPI_Irecv(input[j], COLS, MPI_FLOAT,
               	0,j , MPI_COMM_WORLD, &request);
		if(j!=0)
		{
			computeAverage(myrank,input[j-1],j-1,size,output);
		}	
		MPI_Wait(&request,&status);
		printf("\n%d :: Row %d recieved from 0 with message tag %d",myrank,j+((int)myrank*ROWS/size),j);
		j++;
 }
	computeAverage(myrank,input[j-1],j-1,size,output);
 MPI_Finalize();
 printf("\n%d :: Done\n",myrank);
 return 0;
}

