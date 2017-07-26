
#loading the data
library (ISLR)
attach (Wage)

names(Wage)

dim(Wage)

#loess (local regression)
plot(age ,wage)
title (" Local Regression ")

fit=loess (wage???age ,span =.2, data=Wage) #span is fraction of points to be used for training. It controls flexibility of non-linear fit
summary(fit)

fit2=loess(wage???age ,span =.5, data=Wage)
summary(fit2)


agelims =range(age)
age.grid=seq (from=agelims [1], to=agelims [2])
age.grid
lines(age.grid ,predict (fit ,data.frame(age=age.grid)),col ="red ",lwd =2)
lines(age.grid ,predict (fit2 ,data.frame(age=age.grid)),col =" blue",lwd =2)
legend ("topright",legend =c("Span =0.2" ,"Span =0.5"),col=c("red"," blue"),lty =1, lwd =2, cex =.8)

#The locfit library can also be used for fitting local regression models in R.




