//Implemented by Rishabh Choudhary
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/stat.h>
#include<pwd.h>
#include<sys/types.h>
#include<fcntl.h>
#include<dirent.h>

#define MAXSELPARAMCOUNT 4

/*Custom error*/
int errorMessage(char * Message)
{
        printf("%s\n",Message);
        return 1;
}

/*Selection Param Section Begins*/

/*Global variables for current Selection criterion*/
char * ParamAttrib=NULL;
char * ParamAttribVal=NULL;

/*Selection Params*/
char *SelectionParam[MAXSELPARAMCOUNT];
int (*SelParamFunc[MAXSELPARAMCOUNT])(char *,struct stat);
int SelParamCount;

/*Add and configure Selection Param*/
int addSelParam(char * Param,int (*ParamFunc)(char*,struct stat))
{
	if(SelParamCount>=MAXSELPARAMCOUNT) return errorMessage("Max Selection Param count reached");
	if(Param==NULL)
	{
		SelectionParam[SelParamCount]=NULL;
	}
	else
	{	
		SelectionParam[SelParamCount]=(char*)malloc(sizeof(char)*strlen(Param));
		if(SelectionParam[SelParamCount]==NULL) return errorMessage("Out of memory");
		strcpy(SelectionParam[SelParamCount],Param);
	}
	SelParamFunc[SelParamCount++]=ParamFunc;
	return 0;
}
	

/*Procedure to match name*/
int matchName(char* Name,struct stat Buf)
{
	return strcasecmp(Name,ParamAttribVal);
}

/*Procedure to match mtime*/
int matchMTime(char* Name,struct stat Buf)
{
	if(time(NULL)-Buf.st_mtime<=atoi(ParamAttribVal)*24*60*60) return 0;
	else return 1; 
}

/*Procedure to match User ID*/
int matchUser(char * Name,struct stat Buf)
{
	return strcmp((getpwuid(Buf.st_uid)!=NULL?getpwuid(Buf.st_uid)->pw_name:""),ParamAttribVal);
}
	
/*Check if selection is satisfied*/
int matchSelection(char * Name,struct stat Buf)
{
	int i;	
	if(ParamAttrib==NULL) return 0;
	for(i=0;SelectionParam[i]!=NULL;i++)
		if(!strcmp(ParamAttrib,SelectionParam[i]))
			return (*SelParamFunc[i])(Name,Buf);
	return 1;
}

/*Selection Param Section Ends*/

/*Command Section Begins*/

/*Shell script for Command execution*/
FILE * ShellScript=NULL;

/*Open a File*/
int openFile(FILE **FilePtr,char * FileName,char * Mode)
{
	*FilePtr=fopen(FileName,Mode);	
	if (*FilePtr == NULL)
	{
		return errorMessage("Error opening file");
	}
	else
		return 0;
}

/*Command storage variables*/
char **CommandList=NULL;
int CommandListCount;
int PathIndex;

/*Execute command*/	
int runCommand(char * FilePath)
{
	int i;
	if(CommandList==NULL) return 0;
	CommandList[PathIndex]=(char*)malloc(sizeof(char)*strlen(FilePath));
	if(CommandList[PathIndex]==NULL) return errorMessage("Out of memory"); 
	strcpy(CommandList[PathIndex],FilePath);
	for(i=0;i<CommandListCount;i++)
		fprintf(ShellScript,"%s ",CommandList[i]);
	fprintf(ShellScript,"\n");
	return 0;
}

/*Command Section Ends*/

/*Process Arguments*/
int ProcessArguments(int ArgCount,char ** Args)
{
	int i,PathIndexSet=0;
	if(ArgCount<2) return errorMessage("Source missing");
	if(ArgCount==2) printf("Source=%s\n\n",Args[1]);
	if(ArgCount==3) return errorMessage("Selection criterion incomplete");
	if(ArgCount>3)
	{
		printf("Source=%s, Selection=%s, Argument=%s\n\n",Args[1],Args[2],Args[3]);
		for(i=0;SelectionParam[i]!=NULL;i++)
		{
			if(!strcmp(SelectionParam[i],Args[2]))
			{
				ParamAttrib=(char*)malloc(strlen(Args[2])*sizeof(char));
				if(ParamAttrib==NULL) return errorMessage("Out of memory");
				ParamAttribVal=(char*)malloc(strlen(Args[3])*sizeof(char));
				if(ParamAttribVal==NULL) return errorMessage("Out of memory");
				strcpy(ParamAttrib,Args[2]);
				strcpy(ParamAttribVal,Args[3]);
				break;
			}
		}
		if(SelectionParam[i]==NULL) return errorMessage("Invalid Selection criterion");
	}
	if(ArgCount>4)
	{
		CommandListCount=ArgCount-4+1;
		CommandList=(char **)malloc((CommandListCount)*sizeof(char*));
		if(CommandList==NULL)
			return errorMessage("Out of memory");
		i=0;
		CommandList[i]=(char*)malloc(sizeof(char)*(strlen(Args[i+4])-1));
		strcpy(CommandList[i],Args[i+4]+1);
		while((++i)+4<ArgCount)
		{
				if(Args[i+4][0]=='-')	
				{
					CommandList[i]=(char *)malloc(sizeof(char)*strlen(Args[i+4]));
					if(CommandList[i]==NULL) return errorMessage("Out of memory");	
					strcpy(CommandList[i],Args[i+4]);
				}
				else 
				{
					PathIndex=i;
					PathIndexSet=1;	
					if(i+4==ArgCount-1)
					{
						CommandList[i+1]=(char *)malloc(sizeof(char)*strlen(Args[i+4]));
						if(CommandList[i+1]==NULL) return errorMessage("Out of memory");	
						strcpy(CommandList[i+1],Args[i+4]);
					}
					else return errorMessage("Command incorrect");
				}
		}
		if(!PathIndexSet) PathIndex=i;
	}
	return 0;
}

/*Entry Process Section Begins*/

/*read stat for a file or directory using stat*/
int readFileStats(char * FileName,struct stat * Buf) 
{
	if((lstat(FileName, Buf))!=-1)
		return 0;
	else
		return 1;	
}

/*Test file type of the file*/ 
int testFileType(struct stat Buf, int * FileType,int TestType)
{
	*FileType = Buf.st_mode & 0xF000;
	if(*FileType==TestType)
		return 0;
	else
		return 1;
}

/*Function to print Dir Entry*/
void printDirEntry(char * EntryType,char * EntryName)
{
	printf("%-10s %-20s\n",EntryType,EntryName);
}

/*Process directory*/
int processDirectory(char * FileName,char * FilePath,struct stat Buf)
{
	int visitDirectory(char*);
	if(!matchSelection(FileName,Buf))	
		printDirEntry("Dir",FilePath);
	visitDirectory(FilePath);
	return 0;
}
	
/*Process non directory- Reg and other files*/
int processNonDirectory(char * FileName,char * FilePath,struct stat Buf,char * FileTypeString)
{
	if(!matchSelection(FileName,Buf))
	{
		printDirEntry(FileTypeString,FilePath);
		runCommand(FilePath);
	}
	return 0;
}

/*Check and process entry*/
int processEntry(char * FilePath,char * FileName)
{
	struct stat Buf;
	int FileType;
	if(!readFileStats(FilePath,&Buf))
	{
		if(!testFileType(Buf,&FileType,S_IFDIR))
			processDirectory(FileName,FilePath,Buf);
		else if(!testFileType(Buf,&FileType,S_IFREG))
			processNonDirectory(FileName,FilePath,Buf,"Reg");
		else 	
			processNonDirectory(FileName,FilePath,Buf,"Oth");
		return 0;
	}
	else 
	{
		perror(FilePath);
		return 1;
	}
	return 0;
}

/*Procedure called in case of directory*/
int visitDirectory(char * DirectoryName)
{
	struct dirent * DirEntry;
	DIR * DirectoryPtr;
	char * FilePath;
	if((DirectoryPtr=opendir(DirectoryName))!=NULL)
	{
		while((DirEntry=readdir(DirectoryPtr))!=NULL)
		{
			if(!strcmp(DirEntry->d_name,".") || !strcmp(DirEntry->d_name,"..")) continue;
			FilePath=(char*)malloc(strlen(DirectoryName)+strlen(DirEntry->d_name)+2);
			if(FilePath==NULL) return errorMessage("Out of memory"); 
			sprintf(FilePath,"%s/%s",DirectoryName,DirEntry->d_name);	
			processEntry(FilePath,DirEntry->d_name);
			free(FilePath);
			free(DirEntry);
		}
		closedir(DirectoryPtr);
		free(DirectoryPtr);
	}
	else
	{
		perror(DirectoryName);
		return 1;
	}	
	return 0;
}	

/*Entry Process Section Ends*/
	
int main(int argc, char * argv[])
{
	int FileType;
	struct stat Buf;
	if(openFile(&ShellScript,"CommandExec.sh","w")) return 1;
	if(addSelParam("-name",matchName)) return 1;
	if(addSelParam("-mtime",matchMTime)) return 1;
	if(addSelParam("-user",matchUser)) return 1;
	if(addSelParam(NULL,NULL)) return 1;
	if(ProcessArguments(argc,argv)) return 1;
        if(!readFileStats(argv[1],&Buf))
        {
                if(!testFileType(Buf,&FileType,S_IFDIR))
                        processDirectory(argv[1],argv[1],Buf);
                else
			return errorMessage("Source is not a directory");
	}
	else
	{
		perror(argv[1]);
		return 1;
	}
	fclose(ShellScript);
	system("sh -f CommandExec.sh");
	system("rm CommandExec.sh");
	return 0;
}


