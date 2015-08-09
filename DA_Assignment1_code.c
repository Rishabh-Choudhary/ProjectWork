//Implemented by Rishabh Choudhary
#include<stdio.h>
#include<math.h>
#include<string.h>
#include<stdlib.h>
#define NUMCLASS 50
#define CLASSNMLEN 30
#define NUMDATAROW 3000
#define DATAROWLEN 100

/*Array of boundary points*/
int BoundaryPoint[NUMDATAROW];

/*Array of class labels*/
char *ClassLabel[NUMCLASS]; 

/*variable to store number of Class labels read*/	
int ClassLabelsRead;

/*structure array to store data rows*/ 
struct DataRowType 
{
	float NumValue; //Feature value from the data row
	char *ClassLabel; //Class labels from the data row 
} DataRow[NUMDATAROW];

/*Function to print datarows - not used, just for test */
void printDataRow(struct DataRowType DataRow)
{
	printf("%lf %s\n",DataRow.NumValue,DataRow.ClassLabel);
}

/*variable to store number of Data rows read*/	
int DataRowsRead;

void display(int BoundaryPointsFound,char * Message)
{
	/*display function for the program*/
	int i,BoundaryPointIndex=0;
	system("clear");

	/*display title*/
	printf(" ");	
	for(i=0;i<104;i++)
		printf("_");
	printf("\n|%36s%-68s|\n"," ","Discretization - Fayyad & Irani");

	/*display number of class labels and name of each*/	
	printf("|");	
	for(i=0;i<104;i++)
		printf("_");
	printf("|");	
	printf("\n|%-15s%-8d%81s|\n","ClassLabels:",ClassLabelsRead," ");
	printf("|");	
	for(i=0;i<40;i++)
		printf("_");
	printf("%64s|\n"," ");
	printf("| %-8s|%-30s|%63s|\n","No.","Classlabel"," ");
	printf("|");	
	for(i=0;i<40;i++)
		printf("_");
	printf("|%63s|\n"," ");
	for(i=0;i<ClassLabelsRead;i++)
		printf("| %-8d|%-30s|%63s|\n",i+1,ClassLabel[i]," ");
	printf("|");	
	for(i=0;i<40;i++)
		printf("_");
	printf("|%63s|\n"," ");
	printf("|%104s|"," ");

	/*display number of data rows read*/ 
	printf("\n|%-20s%-8d%76s|\n","Data Rows in S:",DataRowsRead," ");
	printf("|%104s|"," ");

	/*display boundary point information*/
	printf("\n|%-25s%-8d%71s|\n","Boundary Points Found:",BoundaryPointsFound," ");
	printf("|");	
	for(i=0;i<104;i++)
		printf("_");
	printf("|");
	
	/*display table containing intervals as shown in weka, boundary points, interval as per the values, count of datarows in each interval*/ 
	printf("\n| %-9s| %-29s | %-18s| %-29s | %-8s|\n","Set No.","Interval (Weka)","Boundary Point","Interval (By values)","Count");
	printf("|");	
	for(i=0;i<104;i++)
		printf("_");
	printf("|");	
	if(BoundaryPointsFound==0)
		printf("\n| S%-8d| (%12s - %-12s) | %18s| [%12f - %-12f] | %8d|\n",1,"-inf","inf"," ", DataRow[0].NumValue, DataRow[DataRowsRead-1].NumValue,DataRowsRead);	
	else
	{
		printf("\n| S%-8d| (%12s - %-12f] | %18f| [%12f - %-12f] | %8d|\n",BoundaryPointIndex+1,"-inf",(DataRow[BoundaryPoint[BoundaryPointIndex]].NumValue+DataRow[BoundaryPoint[BoundaryPointIndex]+1].NumValue)/2,DataRow[BoundaryPoint[BoundaryPointIndex]].NumValue, DataRow[0].NumValue, DataRow[BoundaryPoint[BoundaryPointIndex]].NumValue,BoundaryPoint[BoundaryPointIndex]+1);
		BoundaryPointIndex++;
		while(BoundaryPointIndex<BoundaryPointsFound)
		{
			printf("| S%-8d| (%12f - %-12f] | %18f| [%12f - %-12f] | %8d|\n",BoundaryPointIndex+1,(DataRow[BoundaryPoint[BoundaryPointIndex-1]].NumValue+DataRow[BoundaryPoint[BoundaryPointIndex-1]+1].NumValue)/2,(DataRow[BoundaryPoint[BoundaryPointIndex]].NumValue+DataRow[BoundaryPoint[BoundaryPointIndex]+1].NumValue)/2,DataRow[BoundaryPoint[BoundaryPointIndex]].NumValue,DataRow[BoundaryPoint[BoundaryPointIndex-1]+1].NumValue,DataRow[BoundaryPoint[BoundaryPointIndex]].NumValue,BoundaryPoint[BoundaryPointIndex]-BoundaryPoint[BoundaryPointIndex-1]);
			BoundaryPointIndex++;
		}
		printf("| S%-8d| (%12f - %-12s) | %18s| [%12f - %-12f] | %8d|\n",BoundaryPointIndex+1,(DataRow[BoundaryPoint[BoundaryPointIndex-1]].NumValue+DataRow[BoundaryPoint[BoundaryPointIndex-1]+1].NumValue)/2,"inf"," ",DataRow[BoundaryPoint[BoundaryPointIndex-1]+1].NumValue,DataRow[DataRowsRead-1].NumValue,DataRowsRead-1-BoundaryPoint[BoundaryPointIndex-1]);
	}
	printf("|");	
	for(i=0;i<104;i++)
		printf("_");
	printf("|");

	/*display message*/	
	printf("\n|%-10s%-94s|\n","Message:",Message);
	printf("|");	
	for(i=0;i<104;i++)
		printf("_");
	printf("|");	
	printf("\n");
}

int readClassLabels(FILE * Fptr)
{
	/*read and store class labels from the first line of the input file into the array ClassLabel and store the count into ClassLabelsRead*/    
	char ClassLabelString[(NUMCLASS*CLASSNMLEN)+NUMCLASS];
	char TmpClassLabel[CLASSNMLEN];
	int i,j;
	memset(ClassLabelString,'\0',sizeof(ClassLabelString));
	memset(TmpClassLabel,'\0',sizeof(TmpClassLabel));
	fgets(ClassLabelString,NUMCLASS*CLASSNMLEN,Fptr);
	sscanf(ClassLabelString,"%*s %s\n",ClassLabelString);
	j=0;
	display(0,"Reading Class labels");
	ClassLabelsRead=0;	
	for(i=0;ClassLabelString[i]!='\0';i++)
	{
		if(ClassLabelString[i]==',')
		{
			ClassLabel[ClassLabelsRead]=(char*)malloc(j*sizeof(char)); 
			memset(ClassLabel[ClassLabelsRead],'\0',sizeof(ClassLabel[ClassLabelsRead]));
			strncpy(ClassLabel[ClassLabelsRead],TmpClassLabel,j);
			ClassLabelsRead++;
			j=0;
			memset(TmpClassLabel,'\0',sizeof(TmpClassLabel));
		}
		else
			TmpClassLabel[j++]=ClassLabelString[i];
	} 
	ClassLabel[ClassLabelsRead]=(char*)malloc(j*sizeof(char));
	if(ClassLabel[ClassLabelsRead]==NULL)
	{
		printf("Out of Memory\n");
		return 1;
	} 
	strncpy(ClassLabel[ClassLabelsRead],TmpClassLabel,j);
	ClassLabelsRead++;
	display(0,"Done reading Class labels"); 
	return 0;
}

int readDataRows(FILE * Fptr)
{
	/*read and store data rows from the input file into the structure array DataRow and store the count of data rows in DataRowsRead*/
	char TmpDataRow[DATAROWLEN],TmpClassLabel[CLASSNMLEN];
	display(0,"Reading Data rows");
	DataRowsRead=0;
	while(1)
	{
		memset(TmpDataRow,'\0',sizeof(TmpDataRow));
		fgets(TmpDataRow,DATAROWLEN,Fptr);
		if(feof(Fptr)) break;
		if(strncasecmp(TmpDataRow,"end",3)==0 || strncasecmp(TmpDataRow,"data",4)==0) continue;
		memset(TmpClassLabel,'\0',sizeof(TmpClassLabel));
		sscanf(TmpDataRow,"%f,%s\n",&DataRow[DataRowsRead].NumValue,TmpClassLabel);
		DataRow[DataRowsRead].ClassLabel=(char*)malloc(strlen(TmpClassLabel)*sizeof(char));
		if(DataRow[DataRowsRead].ClassLabel==NULL)
		{
			printf("Out of Memory\n");
			return 1;
		}
		memset(DataRow[DataRowsRead].ClassLabel,'\0',sizeof(DataRow[DataRowsRead].ClassLabel));
		strncpy(DataRow[DataRowsRead].ClassLabel,TmpClassLabel,strlen(TmpClassLabel));
		DataRowsRead++;
	}
	display(0,"Done reading Data rows");
	return 0;
}

int readFile(char * FileName)
{
       	/*Open and read input file and store data in data structures*/ 
	FILE * Fptr;
        Fptr=fopen(FileName,"r");
        if(Fptr==NULL)
        {
                printf("Invalid File name\n");
                return 1;
        }
	display(0,"File Opened");	
	display(0,"Reading file");	
        if(readClassLabels(Fptr)) return 1;
        if(readDataRows(Fptr)) return 1;
        fclose(Fptr); 
	display(0,"Closing file");	
	return 0;
}

int sortDataRows()
{
	/*sort data rows in the structure array in ascending order of the feature*/
	int i,j;
	struct DataRowType TmpDataRow;
	display(0,"Sorting Data rows in ascending order");
	for(i=0;i<DataRowsRead;i++)
		for(j=i+1;j<DataRowsRead;j++)
			if(DataRow[i].NumValue>DataRow[j].NumValue || (DataRow[i].NumValue==DataRow[j].NumValue && strcmp(DataRow[i].ClassLabel,DataRow[j].ClassLabel)<0))
			{
				TmpDataRow=DataRow[j];
				DataRow[j]=DataRow[i];
				DataRow[i]=TmpDataRow;
			}
	display(0,"Done sorting Data rows");
	return 0; 
}

int indexOfClass(char * TmpClassLabel)
{
	/*gets index of the class label in the array ClassLabel. Used by entropy to calculate probability*/
	int i;
	for(i=0;i<ClassLabelsRead;i++)
		if(!strcmp(TmpClassLabel,ClassLabel[i])) return i;
	return -1;
} 

double entropy(int Start,int End)
{
	/*Calculate entropy*/	
	int i;
	double EntropyVal=0.0,TotalClass=End-Start+1;
	double ClassCount[ClassLabelsRead]; 	
	for(i=0;i<ClassLabelsRead;i++)
		ClassCount[i]=0;	
	for(i=Start;i<=End;i++)
		ClassCount[indexOfClass(DataRow[i].ClassLabel)]++;
	for(i=0;i<ClassLabelsRead;i++)
		if((ClassCount[i]!=0) && (ClassCount[i]/TotalClass!=1))
			EntropyVal+=((ClassCount[i]/TotalClass)*(log(ClassCount[i]/TotalClass)/log(2)));

	return -1*EntropyVal;
}

int distinctClass(int Start,int End)
{
	/*Calculate the value of k used in the formula for delta*/
	int TotalDistinctClass=0,i,ClassPresent[ClassLabelsRead];
	for(i=0;i<ClassLabelsRead;i++)
		ClassPresent[i]=0;	
	for(i=Start;i<=End;i++)
		ClassPresent[indexOfClass(DataRow[i].ClassLabel)]=1;
	for(i=0;i<ClassLabelsRead;i++)
		TotalDistinctClass+=ClassPresent[i];
	return TotalDistinctClass;
}

double delta(int Start,int End,int BoundaryPos)
{
	/*calculate delta*/
	return (log(pow(3,distinctClass(Start,End))-2)/log(2))-((distinctClass(Start,End)*entropy(Start,End))-(distinctClass(Start,BoundaryPos)*entropy(Start,BoundaryPos))-(distinctClass(BoundaryPos+1,End)*entropy(BoundaryPos+1,End)));
}

double informationEntropy(int Start,int End,int BoundaryPos)
{
	/*calculate information entropy*/
	return (((double)(BoundaryPos-Start+1)/(End-Start+1))*entropy(Start,BoundaryPos))+(((double)(End-(BoundaryPos+1)+1)/(End-Start+1))*entropy(BoundaryPos+1,End));
}

double gain(int Start,int End,int BoundaryPos)
{
	/*calculate gain used for checking the partitioning condition*/
	return entropy(Start,End)-informationEntropy(Start,End,BoundaryPos);
}

int boundaryPoints(int Start,int End,int * BoundaryPointIndex)
{

/* calculate boundary points as as per the algorithm*/
/* 1. find min information entropy*/
/* 2. check partitioning contidition*/
/* 3. if yes continue with partition else return*/
/* 4. display boundary points at the end and return*/
 
	int i,BoundaryPos=Start;
	display(*BoundaryPointIndex,"Finding boundary points");	

	/*Find point with minimum Information entropy*/	
	double MinInformationEntropy=informationEntropy(Start,End,BoundaryPos); 
	if(DataRow[Start].NumValue==DataRow[End].NumValue) return 0;
	for(i=Start+1;i<End;i++)
		if(informationEntropy(Start,End,i)<=MinInformationEntropy)
		{	
			MinInformationEntropy=informationEntropy(Start,End,i);
			BoundaryPos=i;
		}

	/*Check partitioning condition*/
	if(!(gain(Start,End,BoundaryPos)<(((log((End-Start+1)-1)/log(2))+delta(Start,End,BoundaryPos))/(double)(End-Start+1))) && DataRow[BoundaryPos].NumValue!=DataRow[BoundaryPos+1].NumValue)
	{
		if(Start!=BoundaryPos)
			if(boundaryPoints(Start,BoundaryPos,BoundaryPointIndex)) return 1;
		BoundaryPoint[(*BoundaryPointIndex)++]=BoundaryPos;
		if(End!=BoundaryPos+1)
			if(boundaryPoints(BoundaryPos+1,End,BoundaryPointIndex)) return 1;
		display(*BoundaryPointIndex,"Done finding boundary points");	
		return 0;
	}
	else return 0;
}

int main(int argc,char *argv[])
{
	/*initialize boundary point counter*/
	int BoundaryPointsFound=0;

	/*read input file. File name supplied as first parameter of the program*/
	if(readFile(argv[1])) return 1;

	/*Sort data rows in descending order of feature*/
	if(sortDataRows()) return 1;

	/*Find and display boundary points*/
	if(boundaryPoints(0,DataRowsRead-1,&BoundaryPointsFound)) return 1;

	return 0;
}

	

