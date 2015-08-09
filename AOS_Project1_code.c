//Implemented by Rishabh Choudhary
#include <stdio.h>
#include <thread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define LOGFILENAME "log"
#define N 10
#define SLEEPTIME 100 
#define WAITTIME 1000 

//Log file pointer
FILE * LogFilePtr;

//Limit for number of rounds with no message
int NumEmpty;

//Network Map data structure
int NMap[N+2][N+2];

//thread id data structures
thread_t tid, thr[N+2];

//total number of peers
int Peers;

//int NumEntryAnyQ=0; //Removed as per intructions in stage7

//lock for each thread
mutex_t Lock[N+2];

//mutex_t NumEntryAnyQLock; //Removed as per instructions in stage7

//structure of message
typedef struct MessageStruct
{unsigned int SenderID;
 unsigned int TTL;
 unsigned int Message;
 struct MessageStruct *NextMessage;
} MessageType, *MsgQptr;

//pointer array for head and tail of queue of each thread
MsgQptr MsgQHead[N+2], MsgQTail[N+2];

//print message
void printMessage(MessageType MessageHolder)
{
	printf("|SenderID: %3d|TTL: %3d|Message :%5d|\n",MessageHolder.SenderID,MessageHolder.TTL,MessageHolder.Message);
}

//Initialize message queue 
int initiatizeMsgQ()
{
	memset(MsgQHead,NULL,sizeof(MsgQHead));
	memset(MsgQTail,NULL,sizeof(MsgQTail));
	return 0;
}

//insert message in the queue
int putMsg(MsgQptr MessagePtr,int ReceiverID)
{
	mutex_lock(&Lock[ReceiverID]);
	if(MsgQHead[ReceiverID]==NULL && MsgQTail[ReceiverID]==NULL)
	{
		MsgQHead[ReceiverID]=MessagePtr;
		MsgQTail[ReceiverID]=MessagePtr;
	}
	else
	{
		MsgQTail[ReceiverID]->NextMessage=MessagePtr;
		MsgQTail[ReceiverID]=MsgQTail[ReceiverID]->NextMessage;
	}
	mutex_unlock(&Lock[ReceiverID]);
//	mutex_lock(&NumEntryAnyQLock);	//Removed as per stage7 instructions
//	NumEntryAnyQ++;			//Removed as per stage7 instructions
//	mutex_unlock(&NumEntryAnyQLock);//Removed as per stage7 instructions	
	return 0;
}

//create message and add to queue
int sendMsg(int SenderID, int ReceiverID, int Message, int TTL)
{
	MsgQptr MessagePtr=(MsgQptr)malloc(sizeof(MessageType));
	if(MessagePtr==NULL)
		return 1;
	MessagePtr->SenderID=SenderID;
	MessagePtr->TTL=TTL;
	MessagePtr->Message=Message;
	MessagePtr->NextMessage=NULL;
	if(putMsg(MessagePtr,ReceiverID)) return 1;
	return 0;
}

//get message from queue
MsgQptr getMsg(int ReceiverID)
{
	MsgQptr MessagePtr=NULL;	
	mutex_lock(&Lock[ReceiverID]);
	if(MsgQHead[ReceiverID]!=NULL)
	{
		MessagePtr=MsgQHead[ReceiverID];
		if(MessagePtr->NextMessage!=NULL)	
			MsgQHead[ReceiverID]=MessagePtr->NextMessage;
		else
		{
			MsgQHead[ReceiverID]=NULL;
			MsgQTail[ReceiverID]=NULL;
		}
		MessagePtr->NextMessage=NULL;
	}
	mutex_unlock(&Lock[ReceiverID]);
//	mutex_lock(&NumEntryAnyQLock);	//Removed as per stage7 instructions
//	if(MessagePtr!=NULL) NumEntryAnyQ--;//Removed as per stage7 instructions
//	mutex_unlock(&NumEntryAnyQLock);//Removed as per stage7 instructions
	return MessagePtr;
}

//print Network Map
void printNMap()
{
        int i,j;
	printf("\nNetwork Map:\n\n");
        printf("%3s|"," ");
        for(i=0;i<Peers+2;i++)
                printf("%3d",i);
        printf("\n");
        printf("%3s|","---");
        for(i=0;i<Peers+2;i++)
                printf("%3s","---");
        printf("\n");
        for(i=0;i<Peers+2;i++)
        {
                printf("%-3d|",i);
                for(j=0;j<Peers+2;j++)
                        printf("%3d",NMap[i][j]);
                printf("\n");
        }
	printf("\n");
}

//read numbers from a line. used to read number of neighbours and neighbour id  
int readNumFromLine(char * Line,int * Pos,char Delim,int * Num)
{
        *Num=0;
        while(*Pos<strlen(Line)-1)
        {
                if(Line[(*Pos)]==Delim)
                {
                        (*Pos)++;
                        continue;
                }
                else break;
        }

        while(*Pos<strlen(Line)-1)
        {
                *Num=((*Num)*10)+((int)Line[(*Pos)++]-48);
                if(Line[*Pos]==Delim || Line[*Pos]=='\0' || Line[*Pos]=='\n') 
			return 0;
        }
        return 1;
}

//read network map
int readNetConn(FILE ** fptr)
{
        int i,j,NeighbourId,Neighbours;
        char Line[20];
        memset(NMap,0,sizeof(NMap));
        fgets(Line,20,*fptr);
        Peers=atoi(Line);
        for(i=1;i<=Peers;i++)
        {
                fgets(Line,20,*fptr);
                j=0;
                if(readNumFromLine(Line,&j,' ',&Neighbours)) continue;
                while(!readNumFromLine(Line,&j,' ',&NeighbourId) && Neighbours)
                {
                                NMap[i][NeighbourId]=1;
                                NMap[NeighbourId][i]=1;
                                Neighbours--;
                }
        }
        return 0;
}

//function to open a file
int openFile(FILE **FilePtr, char * FileName,char * Mode)
{
	if(FileName==NULL)
	{
		printf("Invalid file name\n");
		return 1;
	}
	*FilePtr=fopen(FileName,Mode);
	if(*FilePtr==NULL) return 1;
	return 0;
}

//function to be used by threads
void *peer(void * arg)
{
	MsgQptr MessagePtr;
	int i,ThreadId=(int)arg,NoMsgRoundCnt=0;
	float SumNoMsgRndCnt=0,Round=0;
	int NeighbourNode[Peers+2];
	for(i=0;i<Peers+2;i++)
		NeighbourNode[i]=NMap[ThreadId][i];
	printf("%-6s : %-4d : %s : ","Thread",ThreadId, "Neighbours");
	for(i=0;i<Peers+2;i++)
		printf("%d ",NeighbourNode[i]);
	printf("\n");
	usleep(SLEEPTIME);
	while(NoMsgRoundCnt<NumEmpty)
	{
		if(MsgQHead[ThreadId]==NULL) NoMsgRoundCnt++;
		else
		{
			if(NoMsgRoundCnt) Round++;
			SumNoMsgRndCnt+=NoMsgRoundCnt;
			NoMsgRoundCnt=0;	
			while(MsgQHead[ThreadId]!=NULL)
			{
				MessagePtr=getMsg(ThreadId);
				if(MessagePtr!=NULL)
				{
					if((MessagePtr->TTL)>=1)
					{
						(MessagePtr->TTL)--;
						for(i=0;i<Peers+2;i++)
						{
							if(i==MessagePtr->SenderID) continue;
							if(NeighbourNode[i]==1) 
							{
								sendMsg(ThreadId,i,MessagePtr->Message,MessagePtr->TTL);
								fprintf(LogFilePtr,"%d %d %d %d %d\n",MessagePtr->SenderID,ThreadId,i,MessagePtr->Message,MessagePtr->TTL);
							}
						}
					}
				} 
				free(MessagePtr);
				usleep(SLEEPTIME);
				//if(NumEntryAnyQ<=0) break;//Removed as per Instructions stage7
			}	
		}
		usleep(WAITTIME);
	}
	printf("%-6s : %-4d : %s : ","Thread",ThreadId,"Avg no. of empty Q rounds");
	if(Round) printf("%f\n",SumNoMsgRndCnt/Round);
	else printf("%f\n",Round);
	return NULL;
}

//broadcast messages
int broadcastMsg(FILE ** FilePtr)
{
	char Line[20];
	int Message,TTL;
	while(1)
	{
		fgets(Line,20,*FilePtr);
		if(feof(*FilePtr)) break;
		sscanf(Line,"%d %d\n",&Message,&TTL);
		if(sendMsg(1,2,Message,TTL)) return 1;
		usleep(WAITTIME);
	}
	return 0;
}

//Count Number of messages in a queue
int countQMsg(int ThreadId)
{
        int count=0;
        MsgQptr x;
        x=MsgQHead[ThreadId];
        while(x!=NULL)
        {
                x=x->NextMessage;
                count++;
        }
        return count;
}

int setNumEmpty(char * NumEmptyVal)
{	
	if(NumEmptyVal==NULL)
	{
		printf("Value for NumEmpty not provided\n");
		return 1;
	}
	NumEmpty=atoi(NumEmptyVal);
	return 0;
}

int main(int argc,char *argv[])
{
	int i,status;
	FILE * FilePtr;
	if(setNumEmpty(argv[2])) return 1;
	if(openFile(&FilePtr,argv[1],"r")) return 1;
	if(readNetConn(&FilePtr)) return 1;
	printNMap();	
	if(openFile(&LogFilePtr,LOGFILENAME,"w")) return 1;
	for (i=2; i<Peers+2; i++)
 		thr_create(NULL, 0, peer, (void*)i, THR_BOUND, &thr[i]);

	if(broadcastMsg(&FilePtr)) return 1;	

	while (thr_join(0, &tid, (void**)&status)==0)
		printf("%-6s : %-4d : %s : %d\n","Thread",tid, "Exited with status",status);
	
	for(i=0;i<N+2;i++)
	{
		if(MsgQHead[i]!=NULL)
		{
			printf("%-6s : %-4d : %s : %d\n","Queue",i,"Messages remaining",countQMsg(i));
                        printf("%-6s : %-4d : %s : ","Queue",i,"Message on top");
                        printMessage(*MsgQHead[i]);	
		}
		else
                        printf("%-6s : %-4d : %s \n","Queue",i,"Empty");	
		mutex_destroy(&Lock[i]);
		printf("%-6s : %-4d : %s\n","Lock",i,"Destroyed"); 
	}
	printf("\n");
	fclose(FilePtr);
	fclose(LogFilePtr);
	return 0;
}


