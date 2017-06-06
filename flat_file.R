library(dplyr)
library(gridExtra)
library(RPostgreSQL)
library(reshape2)
library(tidyr)
library(lubridate)
library(caret)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,
                 dbname = "execdwhdb",
                 host = "datawarehouse-vpn.execonline.com",
                 port = 5439, user = "sysadmin",
                 password = )
courses <- dbGetQuery(con,"SELECT * FROM execprod.dimcourse")
survey_ans <- dbGetQuery(con,"SELECT * FROM execprod.stg_sg_survey_response WHERE sk_survey = 82")
# get dimuser table
dimuser <- dbGetQuery(con,"SELECT * FROM execprod.dimuser")
#student data
students <- dbGetQuery(con,"SELECT * FROM execprod.stg_sf_students")
#programs <- dbGetQuery(con, c("SELECT * FROM execprod.stg_sf_programs"))
programs <- dbGetQuery(con, c("SELECT id, course_start_date FROM execprod.stg_sf_programs"))


##############################################################################

# clean up student data #

###
### STEP 1 ###
###

# select students who completed or graduated
students <- filter(students, status == "Completed" | status == "Graduated")


###
### STEP 2 ###
###

# remove records with account QA/EXO/outdated account names
delete_acc <- c("EXO - ACRA Pilot", "EXO - IWS Pilot", "OLD - MIT Sloan Executive Education",
                "outdated 2", "Outdated 1", "QA - org", "QA ACCOUNT", "QA Organization",
                "QA Test Org 22nd Aug", "Execonline, Inc.", "ExecOnline, Inc.", "Yale School Of Management", "")
students <- students[!students$account %in% delete_acc,]
students <- students[,-2]

###
### STEP 3 ###
###

# there are some duplicate records, let's get rid of those 
dup_stud <- students[duplicated(students$contact),]

students <- students[!duplicated(students$contact),]

students2<-students

###
### STEP 4 ###
###

# join the student and dimuser dataframes on contact and salesforce_uid columns, respectively
dup_user <- inner_join(dup_stud, dimuser, by = c("contact" = "salesforce_uid"))

stu_user <- inner_join(students, dimuser,  by = c("contact" = "salesforce_uid"))

# remove duplicate records (a couple of records are duplicated in dimuser so join creates
# duplicates in stu_user)
stu_user <- stu_user[!duplicated(stu_user$contact),]


###
### STEP 5 ###
###
# there are a bunch of records that we lose from students during the merge, i guess they weren't 
# in dimuser?
anti_dup_user <- anti_join(dup_stud, dimuser, by = c("contact" = "salesforce_uid"))

anti_stu_user <- anti_join(students,dimuser,by = c("contact"="salesforce_uid"))

###
### STEP 6 ###
###

# need to shuffle "contact" column in anti_stu_user to 1st row of df else it won't rbind with stu_user

#names(anti_dup_user) <- names(anti_dup_user)[1:96]
names(anti_stu_user)[1:96] <- names(stu_user)[1:96]

# fill x2nd_email_address of anti_stu_user with c(first_name, last_name) to join with dimuser
aj_fnames <- anti_stu_user$first_name
aj_lnames <- anti_stu_user$last_name
aj_names <- paste(aj_fnames,aj_lnames,sep="")
anti_stu_user$x2nd_email_address<-aj_names

aj_fnames <- anti_dup_user$first_name
aj_lnames <- anti_dup_user$last_name
aj_names <- paste(aj_fnames, aj_lnames, sep = "")
anti_dup_user$x2nd_email_address <- aj_names

# join anti_stu_user with dim_user to get attribute from both
# remember: anti_stu_user are records that existed in students table but not dimuser
anti_stu_dim <- inner_join(anti_stu_user,dimuser, by = c("x2nd_email_address" = "name"))
anti_stu_dim <- anti_stu_dim[!duplicated(anti_stu_dim$contact),]
names(anti_stu_dim) <- names(stu_user)

anti_dup_dim <- inner_join(anti_dup_user,dimuser, by = c("x2nd_email_address" = "name"))
anti_dup_dim <- anti_dup_dim[!duplicated(anti_dup_dim$contact),]
names(anti_dup_dim) <- names(dup_user)

students1 <- rbind(stu_user, anti_stu_dim, dup_user, anti_dup_dim)
#students1 <- students1[!duplicated(students1$contact),]

###
### STEP 7 ###
###

#get rid of more test users
delete_vec <- vector()
for (i in 1:nrow(students1)){
  if (sum(strsplit(students1$first_name[i],split="")[[1]][1:4] == c("t","e","s","t"),
          na.rm=T) == 4  | sum(strsplit(students1$first_name[i],split="")[[1]][1:4] == 
                               c("T","E","S","T"),
                               na.rm=T) == 4 |sum(strsplit(students1$first_name[i],split="")[[1]][1:4] == 
                                                  c("T","e","s","t"), na.rm=T) == 4 | 
      sum(strsplit(students1$first_name[i],split="")[[1]][1:3] == c("T","B","D"),
          na.rm=T) == 3){
    delete_vec <- c(delete_vec,i)
  }
}

students1 <- students1[-delete_vec,]

###
### STEP 8 ###
###

students1 <- students1[students1$last_name != "Auditor",]
students1 <- students1[students1$first_name != "Auditor",]
# students1 <- students1[-which(grepl("[[:digit:]]", students1$last_name)),]
# students1 <- students1[-which(grepl("[[:digit:]]", students1$first_name)),]
students2 <- students1

##################################################################################
##################################################################################
###   clean up survey data
##################################################################################
##################################################################################
# subset the survey data to extract only alumni survey questions
#alumni <- filter(survey_ans, sk_survey == 82)

# spread the alumni data to put questions into columns
ans_spread <- spread(survey_ans, question_text, responseitem_text)

# take the important columns from the spreaded answers data
answers_trim <- ans_spread[,c(1, 6, 8:ncol(ans_spread))]

# condense the data so only one row per sf_id, sk_survey,  and submit_dt
answers <- answers_trim %>% group_by(sf_id, sk_survey, submit_dt) %>% summarize_all(funs(toString(unique(.[!is.na(.)]))))

# remove all records with blank sf_id in answers
answers <- filter(answers, sf_id != "")


#####there are some students who have submitted more than 1 survey. for them, let's keep those with the least number of blank answers
#count the number of blanks for each row and attach that vector to the answers data frame
answers.sums <- apply(answers, 1, function(x) sum(x == ""))
answers$not_ans <- answers.sums

# which sf_ids are duplicated in the survey data?
dup_answers_sfid <- answers$sf_id[duplicated(answers$sf_id)]

# subset the survey data to only contain the sf_ids that are duplicated
answers_dups <- filter(answers, sf_id %in% dup_answers_sfid)

# remove all records from survey data frame; we will add the records we want to save later
answers <- filter(answers, !(sf_id %in% dup_answers_sfid))

# pick out the records with minimum number of blank fields for each sf_id
max.index <- answers_dups %>% group_by(sf_id) %>% filter(not_ans == min(not_ans))
max.index <- max.index[-which(duplicated(max.index$sf_id)),]

# add the selected records back to survey data
answers <- data.frame(rbind(answers, max.index))

##### END SURVEY DATA CLEANING #####

##############################################################################################
##############################################################################################
#### CLEAN UP PROGRAM DATA ###################################################################

programs[,-1] <- as.character(programs[,-1])
programs[,-1] <- gsub(pattern = "([0-9]{3})[:3:].*", replacement = "Fall 2013", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:4:]-[:0:][:1:].*|([0-9]{3})[:4:]-[:0:][:2:].*|([0-9]{3})[:4:]-[:0:][:3:].*", replacement = "Winter 2014", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:4:]-[:0:][:4:].*|([0-9]{3})[:4:]-[:0:][:5:].*|([0-9]{3})[:4:]-[:0:][:6:].*", replacement = "Spring 2014", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:4:]-[:0:][:7:].*|([0-9]{3})[:4:]-[:0:][:8:].*|([0-9]{3})[:4:]-[:0:][:9:].*", replacement = "Summer 2014", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:4:]-[:1:][:0:].*|([0-9]{3})[:4:]-[:1:][:1:].*|([0-9]{3})[:4:]-[:1:][:2:].*", replacement = "Fall 2014", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:5:]-[:0:][:1:].*|([0-9]{3})[:5:]-[:0:][:2:].*|([0-9]{3})[:5:]-[:0:][:3:].*", replacement = "Winter 2015", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:5:]-[:0:][:4:].*|([0-9]{3})[:5:]-[:0:][:5:].*|([0-9]{3})[:5:]-[:0:][:6:].*", replacement = "Spring 2015", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:5:]-[:0:][:7:].*|([0-9]{3})[:5:]-[:0:][:8:].*|([0-9]{3})[:5:]-[:0:][:9:].*", replacement = "Summer 2015", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:5:]-[:1:][:0:].*|([0-9]{3})[:5:]-[:1:][:1:].*|([0-9]{3})[:5:]-[:1:][:2:].*", replacement = "Fall 2015", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:6:]-[:0:][:1:].*|([0-9]{3})[:6:]-[:0:][:2:].*|([0-9]{3})[:6:]-[:0:][:3:].*", replacement = "Winter 2016", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:6:]-[:0:][:4:].*|([0-9]{3})[:6:]-[:0:][:5:].*|([0-9]{3})[:6:]-[:0:][:6:].*", replacement = "Spring 2016", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:6:]-[:0:][:7:].*|([0-9]{3})[:6:]-[:0:][:8:].*|([0-9]{3})[:6:]-[:0:][:9:].*", replacement = "Summer 2016", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:6:]-[:1:][:0:].*|([0-9]{3})[:6:]-[:1:][:1:].*|([0-9]{3})[:6:]-[:1:][:2:].*", replacement = "Fall 2016", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:7:]-[:0:][:1:].*|([0-9]{3})[:7:]-[:0:][:2:].*|([0-9]{3})[:7:]-[:0:][:3:].*", replacement = "Winter 2017", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:7:]-[:0:][:4:].*|([0-9]{3})[:7:]-[:0:][:5:].*|([0-9]{3})[:7:]-[:0:][:6:].*", replacement = "Spring 2017", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:7:]-[:0:][:7:].*|([0-9]{3})[:7:]-[:0:][:8:].*|([0-9]{3})[:7:]-[:0:][:9:].*", replacement = "Summer 2017", as.matrix(programs[,-1]))
programs[,-1] <- gsub(pattern = "([0-9]{3})[:7:]-[:1:][:0:].*|([0-9]{3})[:7:]-[:1:][:1:].*|([0-9]{3})[:7:]-[:1:][:2:].*", replacement = "Fall 2017", as.matrix(programs[,-1]))
programs[,-1] <- as.vector(programs[,-1])

program_names <- dbGetQuery(con, "SELECT id, name, course_end_date FROM execprod.stg_sf_programs")
program_names[,-1] <- sapply(program_names[,-1], as.character)
program_names[,-1] <- gsub(pattern = "Columbia\\s[:1:].*", replacement = "Leading Strategic Growth", as.matrix(program_names[,-1]))
program_names[,-1] <- gsub(pattern = "Columbia\\s[:2:].*", replacement = "Embedding Strategic Excellence", as.matrix(program_names[,-1]))
program_names[,-1] <- gsub(pattern = "Columbia\\s[:3:].*", replacement = "Building and Leading Effective Teams", as.matrix(program_names[,-1]))
program_names[,-1] <- gsub(pattern = "Berkeley\\s[:1:].*", replacement = "Leading Innovative Change", as.matrix(program_names[,-1]))
program_names[,-1] <- gsub(pattern = "Berkeley\\s[:2:].*", replacement = "Accelerating Change Readiness and Agility", as.matrix(program_names[,-1]))
program_names[,-1] <- gsub(pattern = "MIT.*", replacement = "Leading Operational Excellence", as.matrix(program_names[,-1]))
program_names[,-1] <- gsub(pattern = "YIMD.*", replacement = "Leading and Managing Globally", as.matrix(program_names[,-1]))
program_names[,-1] <- gsub(pattern = "Columbia_.*_1.*", replacement = "Leading Strategic Growth", as.matrix(program_names[,-1]))
program_names[,-1] <- gsub(pattern = "Columbia_3M_2.*", replacement = "Building and Leading Effective Teams", as.matrix(program_names[,-1]))
program_names[,-1] <- as.vector(program_names[,-1])


programs.final <- inner_join(program_names, programs, by = c("id" = "id"))

##### JOIN SURVEY DATA, STUDENTS DATA, PROGRAM DATA #####
stu_answers <- left_join(students2, answers, by = c("contact" = "sf_id"))
stu_answers <- left_join(stu_answers, programs.final, by = c("program" = "id"))

##### END JOINS #####

##### CLEAN UP THE SURVEY/STUDENT/PROGRAM TABLE #####

### when we did the join of student and survey table, it placed survey data in all records for 
# students who took multiple courses - we need to figure out to which course the survey is actually
# connected. I'll do this by looking at time differences between the course end date and survey
# submission date. We know that surveys are a minimum of 90 post course end date and the last survey is
# 1 year post course end date. We first identify which students have taken more than 1 course. Then
# filter the data by time.diff (the amount of time between the course end date and survey submission).
# For the records in this subset, we disassociate the survey data with 90 > time.diff < 420 days. For
# students who have multiple records that fall within this time frame, we associate the survey data with 
# the minimum number of days since course end date, knowing that it will be > 90

#calculate the number of days between the course end date and survey submission date
stu_answers$course_end_date <- as.POSIXct(strptime(stu_answers$course_end_date, "%Y-%m-%d"))
stu_answers$time.diff <- as.numeric(stu_answers$submit_dt - stu_answers$course_end_date)
       
       
#which contacts are duplicated in the stu_answers table?
dup.contacts <- unique(stu_answers$contact[duplicated(stu_answers$contact)])
#where do those contacts occur in stu_answers
remove.contacts <- which(stu_answers$contact %in% dup.contacts)

#segregate the duplicated contacts into stu_answers_remove - these are the students who took more than 1 course
stu_answers_remove <- stu_answers[remove.contacts,]

#records with fewer than 90 days or NA for time between end of course and survey submission date
under90.or.na <- stu_answers_remove[which(stu_answers_remove$time.diff < 90 | is.na(stu_answers_remove$time.diff)),]
#snag ids of these records so we can remove any survey data for them in stu_answers
under90.or.na.id <- under90.or.na$id
# clean out all survey data from stu_answers based on ids from under90.or.na
na.cols <- match(names(answers)[-1], names(stu_answers))
stu_answers[stu_answers$id %in% under90.or.na.id,][,na.cols] <- NA


#now we need to figure out which of these records don't continue to hold the survey data 
questionable <- filter(stu_answers_remove, !(id %in% under90.or.na.id))
questionable.drop <- questionable %>% group_by(contact) %>% filter(time.diff != min(time.diff))
questionable.drop.id <- questionable.drop$id

# drop survey data for records that are >90 days between time of survey submission and course end date
# these records are the minimum value of time.diff that is greater than 90 days
stu_answers[stu_answers$id %in% questionable.drop.id,][,na.cols] <- NA



###############################
##### cleaning up data
# time/date posixct data types are a pain in the ass - remove columns with this data type to be added back to data
#  frame later
test <- NULL
namelist <- names(stu_answers)
for (j in 1:ncol(stu_answers)){
  t <- class(stu_answers[,namelist[j]])[1] == "POSIXct"
  test <- c(test,t)  
}
# these are the posixct columns
time.dates <- which(test == TRUE)
time.dates.df <- stu_answers[,time.dates]

#this is the data frame with posixct columns removed
no.time.dates <- stu_answers[,-time.dates]
no.time.dates[no.time.dates == ""] <- NA

# change some possible responses
no.time.dates[no.time.dates == "Neither agree or disagree" | 
                no.time.dates == "Neither Likely nor Unlikely"] <- "Neutral"

# these are the column numbers of the implementation percentage questions - we need values
# from these columns to add to the lookup table and will use them later to put all of the
# implementation percentage answers into 1 column
imp.rate.columns <- grep("Roughly.how", names(no.time.dates))
imp.rate.values <- sort(apply(no.time.dates[,imp.rate.columns], 2, unique))

#create a lookup table to determine new values for set responses in survey - this helps
# maintain consistency across all analyses and makes numerical analysis possible
lookup1 <- data.frame(degree = c("Strongly disagree", "Disagree", "Neutral", "Agree", "Strongly agree", 
                                 "Did not meet expectations", "Satisfactory", "Good", "Very Good", "Excellent",
                                 "Highly Unlikely", "Unlikely", "Likely", "Highly Likely", "Unsatisfactory",
                                 imp.rate.values, "Digital was Much Worse", "Worse",
                                 "Neither Better Nor Worse", "Better", "Digital was Much Better",
                                 "No", "Yes", "Much Better", "Much Worse", "No time", "Less than 30 minutes",
                                 "30 minutes to 1 hour", "1-2 hours", "3-4 hours", "5-6 hours", "6-7 hours",
                                 "7-8 hours", "8-9 hours", "10 hours +", "Mrs.",
                                 "Ms.", "Mr.", "Dr.", "0-2 years", "3-5 years", "6-8 years", "9-11 years",
                                 "12-15 years", "16-20 years", "21-25 years", "26-30 years", "31+ years",
                                 "Not possible", "Not Applicable", "N/A", "", NA),
                     new.value = c(1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 4, 5, 1, 5, 3, 4, 2, 1, 1,
                                   2, 3, 4, 5, 0, 1, 5, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 1, 0, 1, 4, 7, 
                                   10, 13.5, 18, 23, 28, 31, NA, NA, NA, NA, NA, NA))

# replace responses to survey questions with appropriate numerical value from lookup table

# some problem-solving necessary - to get these as numeric values, we need to figure out which
# columns we're going to be replacing with values from the lookup table
t <- NULL
for (i in 1:(nrow(lookup1)-3)){
  t <- c(t, unique(unname(which(apply(no.time.dates, 2, function(x) lookup1$degree[i] %in% unique(x)) == T))))
}
#these are the columns where lookup$degree will be replacing at least 1 value
num.cols <- unique(t)

#replace values in no.time.dates with those from lookup1
for (i in 1:nrow(lookup1)){
  no.time.dates[no.time.dates == as.character(lookup1$degree[i])] <- as.numeric(lookup1$new.value[i])
}
#which columns in no.time.dates still have letters? - those that were just replaced will only have numbers,
# but they are still character data type
text.cols <- which(unname(apply(no.time.dates, 2, function(x) sum(grep("[a-z%]", unique(x))))) > 0)

# remove the indexes of columns with letters b/c we won't be converting these to numeric
num.cols <- num.cols[!num.cols %in% text.cols]

# convert all character numbers to numeric numbers
no.time.dates[,num.cols] <- sapply(no.time.dates[,num.cols], as.numeric)

#### we have 5 questions that all deal with percentage of implementation - select them out
# so we can condense into one column, also remove them from the larger data frame - answers
# will be replaced with 1 column (implementation.percentage)

imp.rate <- no.time.dates[,imp.rate.columns]
no.time.dates <- no.time.dates[,-imp.rate.columns]
imp.rate <- sapply(imp.rate, as.numeric)

implementation.percentage <- apply(imp.rate, 1, function(x) sum(x, na.rm=T))
implementation.percentage[implementation.percentage == 0] <- NA
no.time.dates <- cbind(no.time.dates, implementation.percentage)

#######################################
########### More work on columns of survey data

#time management question - need to figure out who answered this and place 1
# where they selected the answer and 0 where they didn't

  #grab all time management question options
  time.management <- grep("time.management.strategies", names(no.time.dates))
  tm.df <- no.time.dates[,time.management]
  #these are the records which have at least one of the options selected
  not.zero <- which(apply(tm.df, 1, function(x) sum(!is.na(x)))!=0)
  # take these records and change NAs in these rows to 0, change selected answers to 1
  tm.df[not.zero,][is.na(tm.df[not.zero,])] <- 0
  tm.df[not.zero,][tm.df[not.zero,] != 0] <- 1
  #substitute transformed data for old data
  no.time.dates[,time.management] <- sapply(tm.df, as.numeric)
  time.management <- grep("time.management.strategies", names(no.time.dates))
  time.management.names <- names(no.time.dates)[time.management]
  names(no.time.dates)[time.management] <- paste("time_management_strategies", seq(1:length(time.management)), sep = "_")
  new.names <- cbind(time.management.names, paste("time_management_strategies", seq(1:length(time.management)), sep = "_"))
  

#barriers to implementation question - need to figure out who answered this and place 1
# where they selected the answer and 0 where they didn't
  
  #grab all time management question options
  barriers <- grep("Which.of.the.following.were.barriers.to.the.successful.implementation", names(no.time.dates))
  barriers.df <- no.time.dates[,barriers]
  #these are the records which have at least one of the options selected
  not.zero <- which(apply(barriers.df, 1, function(x) sum(!is.na(x)))!=0)
  # take these records and change NAs in these rows to 0, change selected answers to 1
  barriers.df[not.zero,][is.na(barriers.df[not.zero,])] <- 0
  barriers.df[not.zero,][barriers.df[not.zero,] != 0] <- 1
  #substitute transformed data for old data
  no.time.dates[,barriers] <- sapply(barriers.df, as.numeric)
  barriers <- grep("Which.of.the.following.were.barriers.to.the.successful.implementation", names(no.time.dates))
  barriers.names <- names(no.time.dates)[barriers]
  names(no.time.dates)[barriers] <- paste("project_barriers", seq(1:length(barriers)), sep = "_")
  new.names <- rbind(new.names, cbind(barriers.names, paste("project_barriers", seq(1:length(barriers)), sep = "_")))
  
  
#aspects of project that program helped with question - need to figure out who answered this and place 1
# where they selected the answer and 0 where they didn't
  
  #grab all aspect question options
  aspects <- grep("What.aspects.of.your.project.did.the.program", names(no.time.dates))
  aspects.df <- no.time.dates[,aspects]
  #these are the records which have at least one of the options selected
  not.zero <- which(apply(aspects.df, 1, function(x) sum(!is.na(x)))!=0)
  # take these records and change NAs in these rows to 0, change selected answers to 1
  aspects.df[not.zero,][is.na(aspects.df[not.zero,])] <- 0
  aspects.df[not.zero,][aspects.df[not.zero,] != 0] <- 1
  #substitute transformed data for old data
  no.time.dates[,aspects] <- sapply(aspects.df, as.numeric)
  aspects <- grep("What.aspects.of.your.project.did.the.program", names(no.time.dates))
  aspects.names <- names(no.time.dates)[aspects]
  names(no.time.dates)[aspects] <- paste("program_aspects_project", seq(1:length(aspects)), sep = "_")
  new.names <- rbind(new.names, cbind(aspects.names, paste("program_aspects_project", seq(1:length(aspects)), sep = "_")))
  
  
#financial impact: cost reduction/growth/both - need to figure out who answered this and place 1
# where they selected the answer and 0 where they didn't
  
  #grab all financial impact question options
  financial <- grep("Will.the.financial.impact.of.your.personal.case", names(no.time.dates))
  financial.df <- no.time.dates[,financial]
  #these are the records which have at least one of the options selected
  not.zero <- which(apply(financial.df, 1, function(x) sum(!is.na(x)))!=0)
  # take these records and change NAs in these rows to 0, change selected answers to 1
  financial.df[not.zero,][is.na(financial.df[not.zero,])] <- 0
  financial.df[not.zero,][financial.df[not.zero,] != 0] <- 1
  #substitute transformed data for old data
  no.time.dates[,financial] <- sapply(financial.df, as.numeric)
  financial <- grep("Will.the.financial.impact.of.your.personal.case", names(no.time.dates))
  financial.names <- names(no.time.dates)[financial]
  names(no.time.dates)[financial] <- paste("financial_impact", seq(1:length(financial)), sep = "_")
  new.names <- rbind(new.names, cbind(financial.names, paste("financial_impact", seq(1:length(financial)), sep = "_")))
  

#digital learning experience - need to convert to numeric
  
  #grab all learning experience question options
  digitalexp <- grep("Now.we.d.like.to.ask.you", names(no.time.dates))
  digitalexp.df <- no.time.dates[,digitalexp]
  digitalexp.df <- sapply(digitalexp.df, as.numeric)
  #substitute transformed data for old data
  no.time.dates[,digitalexp] <- sapply(digitalexp.df, as.numeric) 
  digitalexp <- grep("Now.we.d.like.to.ask.you", names(no.time.dates))
  digitalexp.names <- names(no.time.dates)[digitalexp]
  names(no.time.dates)[digitalexp] <- paste("digital_experience", seq(1:length(digitalexp)), sep = "_")
  new.names <- rbind(new.names, cbind(digitalexp.names, paste("digital_experience", seq(1:length(digitalexp)), sep = "_")))
  

#primary sources of cost reduction - need to figure out who answered this and place 1
# where they selected the answer and 0 where they didn't
  
  #grab all cost reduction question options
  costred <- grep("primary.sources.of.cost.reduction", names(no.time.dates))
  costred.df <- no.time.dates[,costred]
  #these are the records which have at least one of the options selected
  not.zero <- which(apply(costred.df, 1, function(x) sum(!is.na(x)))!=0)
  # take these records and change NAs in these rows to 0, change selected answers to 1
  costred.df[not.zero,][is.na(costred.df[not.zero,])] <- 0
  costred.df[not.zero,][costred.df[not.zero,] != 0] <- 1
  #substitute transformed data for old data
  no.time.dates[,costred] <- sapply(costred.df, as.numeric)
  costred <- grep("primary.sources.of.cost.reduction", names(no.time.dates))
  cost.names <- names(no.time.dates)[costred]
  names(no.time.dates)[costred] <- paste("primary_cost_reduction", seq(1:length(costred)), sep = "_")
  new.names <- rbind(new.names, cbind(cost.names, paste("primary_cost_reduction", seq(1:length(costred)), sep = "_")))
  
  
#primary sources of revenue improvements - need to figure out who answered this and place 1
# where they selected the answer and 0 where they didn't
  
  #grab all cost reduction question options
  revenue <- grep("primary.sources.of.revenue.improvements", names(no.time.dates))
  revenue.df <- no.time.dates[,revenue]
  #these are the records which have at least one of the options selected
  not.zero <- which(apply(revenue.df, 1, function(x) sum(!is.na(x)))!=0)
  # take these records and change NAs in these rows to 0, change selected answers to 1
  revenue.df[not.zero,][is.na(revenue.df[not.zero,])] <- 0
  revenue.df[not.zero,][revenue.df[not.zero,] != 0] <- 1
  #substitute transformed data for old data
  no.time.dates[,revenue] <- sapply(revenue.df,as.numeric)  
  revenue <- grep("primary.sources.of.revenue.improvements", names(no.time.dates))
  revenue.names <- names(no.time.dates)[revenue]
  names(no.time.dates)[revenue] <- paste("primary_revenue", seq(1:length(revenue)), sep = "_")
  new.names <- rbind(new.names, cbind(revenue.names, paste("primary_revenue", seq(1:length(revenue)), sep = "_")))
  
#importance of project - need to figure out who answered this and place 1
# where they selected the answer and 0 where they didn't
  
  #grab all cost reduction question options
  project <- grep("How.important.was.the.business.project", names(no.time.dates))
  project.df <- no.time.dates[,project]
  #these are the records which have at least one of the options selected
  not.zero <- which(apply(project.df, 1, function(x) sum(!is.na(x)))!=0)
  # take these records and change NAs in these rows to 0, change selected answers to 1
  project.df[not.zero,][is.na(project.df[not.zero,])] <- 0
  project.df[not.zero,][project.df[not.zero,] != 0] <- 1
  #substitute transformed data for old data
  no.time.dates[,project] <- sapply(project.df,as.numeric)
  project <- grep("How.important.was.the.business.project", names(no.time.dates))
  project.names <- names(no.time.dates)[project]
  names(no.time.dates)[project] <- paste("project_importance", seq(1:length(project)), sep = "_")
  new.names <- rbind(new.names, cbind(project.names, paste("project_importance", seq(1:length(project)), sep = "_")))
  
  
#recode yes/no questions to be in one column
  resp <- grep("Have.you.expanded.your.responsibilities", names(no.time.dates))
  resp.df <- no.time.dates[,resp]  
  have.you.expanded.responsibilities <- rep(NA, nrow(no.time.dates))
  resp.val <- which(resp.df[,1] == 0 | resp.df[,2] == 1)
  have.you.expanded.responsibilities[resp.val] <- rowSums(resp.df[resp.val,], na.rm = T)
  no.time.dates <- no.time.dates[,-resp]
  no.time.dates <- cbind(no.time.dates, have.you.expanded.responsibilities)

  
  promotion <- grep("Have.you.received.a.promotion", names(no.time.dates))
  promotion.df <- no.time.dates[,promotion]
  have.you.received.a.promotion <- rep(NA, nrow(no.time.dates))
  promotion.val <- which(!is.na(promotion.df[,1]) | !is.na(promotion.df[,2]))
  have.you.received.a.promotion[promotion.val] <- rowSums(promotion.df[promotion.val,], na.rm = T)
  no.time.dates <- no.time.dates[,-promotion]
  no.time.dates <- cbind(no.time.dates, have.you.received.a.promotion)
  promotion <- grep("Have.you.received.a.promotion", names(no.time.dates))
  
  
# delete unnecessary columns
  txt <- c("Write.In.", "Would.you.like.to.nominate","URL.Redirect", "not_ans", "name.y", "x2nd_email_address",
           "anecdotal_feedback", "assignment_rating", "assignments_submitted", "browser", "buddy_2_email", "buddy_2_name",
           "buddy_email", "buddy_name", "city.x", "coach_rating", "contact_type", "current_module", "current_role_yrs",
           "current_segment", "days_since_outreach", "e_s_matrix_type", "email", "escalation_priority", "escalation_type",
           "issue_forum_rating", "last_activity_date", "mailing_list_id", "mobile_phone", "module_survey_source", 
           "office_hours_rating", "overall_attendance", "pair", "payment_method", "platform_username", "post_course_status",
           "ranking", "referral_score", "section", "section_rating", "source", "state.x", "status.x", "stripe_id", 
           "survey_answer_1", "team", "technology_issues", "tech_notes_next_steps", "tech_status", "week_1_assignment",
           "week_1_attendance", "week_2_assignment", "week_2_attendance", "week_3_assignment", "week_3_attendance",
           "week_4_assignment", "week_4_attendance", "week_5_assignment", "week_5_attendance", "week_6_assignment",
           "week_6_attendance", "welcome_webinar_attendance", "lastmodifiedby", "createdbyid", "manager__c", "sk_account",
           "platform_use", "name.x", "currentposition", "division", "timezone", "linked_in_page", "city.y", "state.y",
           "postalcode", "country.y", "status.y", "user_type", "Please.provide.their.names.below", "pre_course_status")
  
  del.cols <- NULL
  for (i in 1:length(txt)){
    del.cols <- c(del.cols, grep(txt[i], names(no.time.dates)))
  }
  
  no.time.dates <- no.time.dates[,-del.cols]
  

#million/billion
  BUbudget <- grep("X.US.Budget", names(no.time.dates))
  BUunits <- grep("X.US.Unit", names(no.time.dates))
  no.time.dates[which(no.time.dates[,BUbudget] == "Not sure"), BUunits] <- NA
  no.time.dates[which(no.time.dates[,BUbudget] == "Not sure"), BUbudget] <- NA
  no.time.dates[which(no.time.dates[,BUunits] == "Million"), BUunits] <- 1000000
  no.time.dates[which(no.time.dates[,BUunits] == "Billion"), BUunits] <- 1000000000
  no.time.dates[,BUunits] <- as.numeric(no.time.dates[,BUunits])
  no.time.dates[grep("<", no.time.dates[,BUbudget]), BUbudget] <- .5
  no.time.dates[,BUbudget] <- as.numeric(no.time.dates[,BUbudget])
  no.time.dates[,BUbudget] <- no.time.dates[,BUbudget] * no.time.dates[,BUunits]
  no.time.dates <- no.time.dates[,-BUunits]
  names(no.time.dates)[BUbudget] <- "business_unit_budget"
  
  BUrevenue <- grep("Revenue.What.is.the.approximate", names(no.time.dates))
  BUrevunits <- grep("Unit.What.is.the.approximate", names(no.time.dates))
  no.time.dates[which(no.time.dates[,BUrevenue] == "Not sure"), BUrevunits] <- NA
  no.time.dates[which(no.time.dates[,BUrevenue] == "Not sure"), BUrevenue] <- NA
  no.time.dates[which(no.time.dates[,BUrevunits] == "Millions"), BUrevunits] <- 1000000
  no.time.dates[which(no.time.dates[,BUrevunits] == "Billions"), BUrevunits] <- 1000000000
  no.time.dates[,BUrevunits] <- as.numeric(no.time.dates[,BUrevunits])
  no.time.dates[grep("<", no.time.dates[,BUrevenue]), BUrevenue] <- .5
  no.time.dates[,BUrevenue] <- as.numeric(no.time.dates[,BUrevenue])
  no.time.dates[,BUrevenue] <- no.time.dates[,BUrevenue] * no.time.dates[,BUrevunits]
  no.time.dates <- no.time.dates[,-BUrevunits]
  names(no.time.dates)[BUrevenue] <- "business_unit_revenue"
  
# primary business impact
  pbi <- grep("What.is.the.primary.business.impact", names(no.time.dates))
  primary.business.impact <- data.frame(primary.business.impact.percent = no.time.dates[,pbi])
  primary.business.impact[primary.business.impact == "Other - Write In"] <- NA
  dmy <- dummyVars("~primary.business.impact.percent", primary.business.impact, sep = "_")
  dummy.pbi <- data.frame(predict(dmy, primary.business.impact))
  #grab the column estimating percentage of improvement for these impacts
  pbi.percent.index <- grep("For.the.item.you.selected.above", names(no.time.dates))
  pbi.percent <- no.time.dates[,pbi.percent.index]
  pbi.percent1 <- gsub("%", "", pbi.percent, fixed = T)
  pbi.percent1 <- as.numeric(pbi.percent1)
  #multiply the percentage increase by the dummy variable data frame to give the percent increase
  # for the proper business impact
  pbi.percent2 <- pbi.percent1 * dummy.pbi
  #remove the original questions and replace with the dummy variable data frame
  no.time.dates <- no.time.dates[, -c(pbi, pbi.percent.index)]
  no.time.dates <- cbind(no.time.dates, pbi.percent2)
  
# percent reduction in costs - convert to numeric
  reduction <- grep("reduction.in.costs", names(no.time.dates))
  no.time.dates[,reduction] <- gsub("<", "", no.time.dates[,reduction])
  no.time.dates[,reduction] <- gsub("%", "", no.time.dates[,reduction])
  no.time.dates[,reduction] <- as.numeric(no.time.dates[,reduction])
  
# percent revenue improvement - convert to numeric
  imprev <- grep("If.possible..please.estimate.the.percentage.improvement.in.revenue.of.your.organization.or.business.unit..which.you.entered.above..that.you.have.realized.thus.far.from.your.personal.case.project..A.rough.estimate.is.fine.", 
                 names(no.time.dates))
  no.time.dates[,imprev] <- gsub("<", "", no.time.dates[,imprev])
  no.time.dates[,imprev] <- gsub("%", "", no.time.dates[,imprev])
  no.time.dates[,imprev] <- as.numeric(no.time.dates[,imprev])
  
  no.time.dates$revenue.improvement.new <- no.time.dates[,imprev] * no.time.dates$business_unit_revenue
  no.time.dates$cost.reduction.new <- no.time.dates[,reduction] * no.time.dates$business_unit_budget
# make dummy variables for "level"
  level <- data.frame(level = no.time.dates$level)
  dmy1 <- dummyVars("~ level", level)
  level.dmy <- data.frame(predict(dmy1, level, sep = "_"))
  no.time.dates <- cbind(no.time.dates, level.dmy)
  no.time.dates <- no.time.dates[,-which(names(no.time.dates) == "level")]
  
# function
  otherfunc <- grep("Other", no.time.dates$function__c)
  no.time.dates$function__c[otherfunc] <- "Other"
  cfunct <- data.frame(cfunct = no.time.dates$function__c)
  dmy2 <- dummyVars("~ cfunct", cfunct)
  cfunct.dmy <- data.frame(predict(dmy2, cfunct, sep = "_"))
  no.time.dates <- cbind(no.time.dates, cfunct.dmy)
  no.time.dates <- no.time.dates[,-which(names(no.time.dates) == "function__c")]

  
# relabel a bunch of columns based on question text
  how.barrier <- grep("How.much.do.you.agree.that.the.following.were.barriers", names(no.time.dates))
  how.barrier.names <- paste("measurement_of_barriers", seq(1:length(how.barrier)), sep = "_")
  how.barrier.quest <- names(no.time.dates)[how.barrier]
  new.names <- rbind(new.names, cbind(how.barrier.quest, how.barrier.names))
  names(no.time.dates)[how.barrier] <- how.barrier.names
  
  curemp <- grep("Approximately.how.long.have.you.been.continuously.working.for.your.current.employer", names(no.time.dates))
  curemp.names <- paste("years_with_current_employer", seq(1:length(curemp)), sep = "_")
  curemp.quest <- names(no.time.dates)[curemp]
  new.names <- rbind(new.names, cbind(curemp.quest, curemp.names))
  names(no.time.dates)[curemp] <- curemp.names
  
  curpos<- grep("Approximately.how.long.have.you.been.continuously.working.in.your.current.position", names(no.time.dates))
  curpos.names <- paste("years_in_current_position", seq(1:length(curpos)), sep = "_")
  curpos.quest <- names(no.time.dates)[curpos]
  new.names <- rbind(new.names, cbind(curpos.quest, curpos.names))
  names(no.time.dates)[curpos] <- curpos.names
  
  perrev <- grep("revenue.you.expect.from.your.personal.case.project.if.fully.successful.", names(no.time.dates))
  no.time.dates[,perrev] <- gsub("%", "", no.time.dates[,perrev])
  no.time.dates[,perrev] <- as.numeric(no.time.dates[,perrev])/100
  
  qs <- which(nchar(names(no.time.dates)) > 80)
  qs.names <- paste("ques", seq(1:length(qs)), sep = "_")
  qs.quest <- names(no.time.dates)[qs]
  new.names <- rbind(new.names, cbind(qs.quest, qs.names))
  names(no.time.dates)[qs] <- qs.names
  

stu_answers <- cbind(no.time.dates, time.dates.df)

del.columns <- c("orientation_date", "pre_orientation_date", "registration_date", "tech_check_submission_date",
                 "tech_check_sent_date", "time_preferences_received_date", "time_preferences_sent_date", "createddate",
                 "lastmodifieddate")
remove.columns <- which(names(stu_answers) %in% del.columns)
stu_answers <- stu_answers[,-remove.columns]

#add in some column names of stu_answers to new.names
old_names <- names(stu_answers)[!names(stu_answers) %in% new.names]
new.names1 <- data.frame(cbind(c(new.names[,2], old_names), c(new.names[,1], old_names)))
names(new.names1) <- c("Column_name", "Question_text")
new.names1 <- arrange(new.names1, Column_name)

stu_answers <- stu_answers[,sort(names(stu_answers))] 

write.csv(lookup1, "lookup.csv")
write.csv(new.names1, "question_text.csv")
write.csv(stu_answers, "stu_answers.csv")



