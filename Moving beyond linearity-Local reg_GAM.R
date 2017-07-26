
#loading the data
library (ISLR)
attach (Wage)

names(Wage)

dim(Wage)

#loess (local regression)
plot(age ,wage)
title (" Local Regression ")

fit=loess (wage~age ,span =.2, data=Wage) #span is fraction of points to be used for training. It controls flexibility of non-linear fit
summary(fit)

fit2=loess(wage~age ,span =.5, data=Wage)
summary(fit2)


agelims =range(age)
age.grid=seq (from=agelims [1], to=agelims [2])
age.grid
lines(age.grid ,predict (fit ,data.frame(age=age.grid)),col ="red ",lwd =2)
lines(age.grid ,predict (fit2 ,data.frame(age=age.grid)),col =" blue",lwd =2)
legend ("topright",legend =c("Span =0.2" ,"Span =0.5"),col=c("red"," blue"),lty =1, lwd =2, cex =.8)

#The locfit library can also be used for fitting local regression models in R.


#Generalized Additive Models (GAMs)

library(gam)

gam.m3 <- gam(wage~s(year,4)+s(age,5)+education,data=Wage)
summary(gam.m3)

par(mfrow=c(1,3))
plot(gam.m3,se=TRUE,col="green")


gam.lo <- gam(wage~s(year,4)+lo(age,span=0.7)+education,data=Wage)
plot(gam.lo,se=TRUE,col="green")

library(akima)
gam.lo.i <- gam(wage~lo(year,age,span=0.5)+education,data=Wage)
plot(gam.lo.i,se=TRUE,col="green")

gam.lr <- gam(I(wage>250)~year+s(age,df=5)+education,faimly="binomial",data=Wage) #logistic regression GAM
par(mfrow=c(1,3))
plot(gam.lr,se=TRUE,col="green")








