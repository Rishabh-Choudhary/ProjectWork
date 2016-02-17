#include<mpi.h>

void recieveMessageFromChildren(int myrank,int size, char * message, int messageTag,MPI_Comm Comm)
{
	MPI_Status status;
 	int leftchild=2*myrank+1;
  	int rightchild=2*myrank+2;
  	if(leftchild<size) 
	{
  		MPI_Recv(message,20,MPI_CHAR,leftchild,messageTag,Comm,&status);
		printf("\n%d :: Message %s recieved from %d",myrank,message,leftchild);	
	}
  	if(rightchild<size)
	{ 
  		MPI_Recv(message,20,MPI_CHAR,rightchild,messageTag,Comm,&status);
		printf("\n%d :: Message %s recieved from %d",myrank,message,rightchild);	
	}
	if(leftchild>=size && rightchild>=size)
		strcpy(message,"Moved in");
}

void sendMessageToChildren(int myrank,int size, char * message, int messageTag,MPI_Comm Comm)
{
	MPI_Status status;
 	int leftchild=2*myrank+1;
  	int rightchild=2*myrank+2;
 	if(leftchild<size) 
  	{	
		MPI_Send(message,20,MPI_CHAR,leftchild,messageTag,Comm);
		printf("\n%d :: Message %s sent to %d",myrank,message,leftchild);	
	}
  	if(rightchild<size)
	{ 
  		MPI_Send(message,20,MPI_CHAR,rightchild,messageTag,Comm);
		printf("\n%d :: Message %s sent to %d",myrank,message,rightchild);	
	}

}

void sendMessageToParent(int myrank,int size, char * message, int messageTag, MPI_Comm Comm)
{
  MPI_Status status;
  int parent=(myrank-1)/2;
  MPI_Send(message,20,MPI_CHAR,parent,messageTag,Comm);
  printf("\n%d :: Message %s sent to %d",myrank,message,parent);	
}

void recieveMessageFromParent(int myrank,int size, char * message, int messageTag, MPI_Comm Comm)
{
  MPI_Status status;
  int parent=(myrank-1)/2;
  MPI_Recv(message,20,MPI_CHAR,parent,messageTag,Comm,&status);
  printf("\n%d :: Message %s recieved from %d",myrank,message,parent);	
}

void barrier(MPI_Comm Comm)
{
 int myrank,size;
 char message[20];
 MPI_Comm_rank(Comm,&myrank);
 MPI_Comm_size(Comm,&size);
 printf("\n%d :: In barrier",myrank);
 if(myrank==0)
 {
	recieveMessageFromChildren(myrank,size,message,1,Comm);
	strcpy(message,"Move out");
	sendMessageToChildren(myrank,size,message,2,Comm);
  	printf("\n%d :: Moving out of barrier",myrank);	
 } 
 else
 {
	recieveMessageFromChildren(myrank,size,message,1,Comm);
	sendMessageToParent(myrank,size,message,1,Comm);
	recieveMessageFromParent(myrank,size,message,2,Comm);
	sendMessageToChildren(myrank,size,message,2,Comm);
  	printf("\n%d :: Moving out of barrier",myrank);	
 } 
}


int main(argc,argv)
int argc;
char **argv;
{
 int myrank;
 MPI_Init(&argc,&argv);
 MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
 barrier(MPI_COMM_WORLD);
 MPI_Finalize();
 printf("\n%d :: Done\n",myrank);
 return 0;
}
