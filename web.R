

library(readr)
library(tidyr)
library(dplyr)
library(smooth)
library(TTR)
library(forecast)
library(data.table)
library(foreach)
library(doParallel) 

dir <- "D:\\Users\\bharadwaj\\Desktop\\DS\\Datasets\\Kaggle\\Web Traffic"
#dir <- "C:\\Users\\Srimala Bharadwaj\\Desktop\\Data science\\Kaggle\\Web Traffic"

train_1 <- fread("train_1.csv")

key_1 <- fread("key_1.csv")

#samp_sub1 <- fread("sample_submission_1.csv")


# extract a lists  of pages and dates
train.date.cols = names(train_1[,-1])

# reshape the training data into long format page, date and views
dt <- melt(train_1,
          d.vars = c("Page"),
          measure.vars = train.date.cols,
          variable.name = "ds",
          value.name = "y")
#######################################################################################################################################
# replace NAs with 0 and calculate page median
dt[is.na(y), y :=0]
dt_sum <- dt[,.(Visits = median(y)) , by = Page]
setkey(dt_sum, Page) #dt_sum data.table is sorted by Page. Accesses "by reference" and is memory efficient


#merge projection dates and key to create submission
key_1[, Page2:= substr(Page, 1, nchar(Page)-11)]
key_1[, Page:= NULL]
setnames(key_1, "Page2", "Page") #rename Page2 to Page
setkey(key_1, Page)  

sub <- merge(key_1, dt_sum, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median.csv")

#######################################################################################################################################
dt_sum1 <- dt[,.(Visits = median(y[(length(y)-59):length(y)])) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="median_2m.csv")


#######################################################################################################################################

# calculate page mean
dt_sum1 <- dt[,.(Visits = mean(y)) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean.csv")

#######################################################################################################################################

func_out_mean <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  mean(y,na.rm=TRUE)
}


# calculate page mean removing outliers
dt_sum1 <- dt[,.(Visits = func_out_mean(y)) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean_out.csv")

#######################################################################################################################################

func_out_mean <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  mean(y,na.rm=TRUE)
}


# calculate page mean removing outliers
dt_sum1 <- dt[,.(Visits = func_out_mean(y[(length(y)-59):length(y)])) , by = Page]
setkey(dt_sum1, Page) #dt_sum1 data.table is sorted by Page. Accesses "by reference" and is memory efficient


sub <- merge(key_1, dt_sum1, all.x = TRUE) #merge based on shared key columns in key_1 and dt_sum

#write output
fwrite(sub[,c('Id', 'Visits')],file="mean_out_2m.csv")

#######################################################################################################################################

#setup parallel backend to use many processors
cores=detectCores()
cl <- makeCluster(cores[1]) #not to overload your computer
registerDoParallel(cl)

system.time({
  
  sma.model <- foreach(i=1:nrow(train_1), .combine=rbind,.packages="smooth","forecast","data.table") %dopar% {
    #sma model
    idx <- which(train_1$Page[i]==key_1$Page)[1]
    test_op <- data.table(Id=key_1$Id[idx:(idx+59)],Visits=sma(tsclean(ts(as.data.table(t(train_1[i,2:551]))$V1, frequency=365,start=c(2015,182))), h=60,silent="graph")$forecast)
    test_op
  }
})

#stop cluster
stopCluster(cl)
#######################################################################################################################################



#setup parallel backend to use many processors
cores=detectCores()
cl <- makeCluster(cores[1]) #not to overload your computer
registerDoParallel(cl)


sma.model <- foreach(i=1:nrow(train_2), .combine=rbind,.packages="smooth","forecast","data.table") %dopar% {
  #sma model
  idx <- which(train_2$Page[i]==key_1$Page)[1]
  test_op <- data.table(Id=key_1$Id[idx:(idx+59)],Visits=sma(tsclean(ts(as.data.table(t(train_2[i,2:551]))$V1, frequency=365,start=c(2015,182))), h=60,silent="graph")$forecast)
  cat(i,";")
  test_op
}

#stop cluster
stopCluster(cl)






key_1_upd <- separate(key_1,"Page",c("Page","Date"),sep=-11)
key_1_upd$Page <- substr(key_1_upd$Page, 1,nchar(key_1_upd$Page)-1) 


for(i in 1:nrow(train1))
{
  #creating data in proper format
  t1 <- as.data.frame(t(train_1[i,2:551]))
  
  #time series - daily
  x <- ts(t1$V1, frequency=365,start=c(2015,182))
  
  #sma model
  SMA_test <- sma(tsclean(x), h=60,silent="graph")
  
  #building output data
  idx <- which(train_1$Page[i]==key_1_upd)[1]
  dir <- "C:\\Users\\Srimala Bharadwaj\\Desktop\\Data science\\AV\\JetRail"
  test_op <- data.frame(Id=key_1_upd[idx:(idx+59),]$Id,Visits=SMA_test$forecast)
  colnames(test_op) <- c("Id","Visits")
  write_csv(test_op, file.path(dir, "smadly.csv"), col_names=ifelse(i %in% 1, TRUE, FALSE), append=TRUE)
  cat(i,";")
}




t1 <- as.data.frame(t(train_1[1,2:551]))

train_1[,-1][1, row.names:=train_1$Page]


#time series - daily
x <- ts(t1$V1, frequency=365,start=c(2015,182))
#sma model
SMA_test <- sma(tsclean(x), h=60,silent="graph")
#building output data
idx <- which(train_1$Page[1]==key_1_upd)[1]
test_op <- data.frame(Id=key_1_upd[idx:(idx+59),]$Id,Visits=SMA_test$forecast)
colnames(test_op) <- c("Id","Visits")
write_csv(test_op, file.path(dir, "smadly.csv"), col_names=ifelse(i %in% 1, TRUE, FALSE), append=TRUE)
cat(1,";")


t1 <- as.data.frame(t(train_1[2,2:551]))
#time series - daily
x <- ts(t1$V1, frequency=365,start=c(2015,182))
#sma model
SMA_test <- sma(tsclean(x), h=60,silent="graph")
#building output data
idx <- which(train_1$Page[2]==key_1_upd)[1]
test_op <- data.frame(Id=key_1_upd[idx:(idx+59),]$Id,Visits=SMA_test$forecast)
colnames(test_op) <- c("Id","Visits")
write_csv(test_op, file.path(dir, "smadly.csv"), col_names=ifelse(i %in% 1, TRUE, FALSE), append=TRUE)
cat(2,";")





