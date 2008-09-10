#ifndef _STATETYPES_H_
#define _STATETYPES_H_

#include <stdlib.h>
#include <string.h>
#define SECS_IN_DAY 86400

typedef int ServiceId;
typedef struct ServicePeriod ServicePeriod;
typedef struct ServiceCalendar ServiceCalendar;
typedef struct Timezone Timezone;
typedef struct TimezonePeriod TimezonePeriod;

struct ServiceCalendar {
    ServicePeriod* head;
} ; 

struct ServicePeriod {
  long begin_time; //the first second on which the service period is valid
  long end_time;   //the last second on which the service_period is valid
  int n_service_ids;
  ServiceId* service_ids;
  ServicePeriod* prev_period;
  ServicePeriod* next_period;
} ;

ServiceCalendar*
scNew( );

void
scAddPeriod( ServiceCalendar* this, ServicePeriod* period );

ServicePeriod*
scPeriodOfOrAfter( ServiceCalendar* this, long time );

ServicePeriod*
scPeriodOfOrBefore( ServiceCalendar* this, long time );

ServicePeriod*
scHead( ServiceCalendar* this );

void
scDestroy( ServiceCalendar* this );

ServicePeriod*
spNew( long begin_time, long end_time, int n_service_ids, ServiceId* service_ids );

void
spDestroyPeriod( ServicePeriod* this );

int
spPeriodHasServiceId( ServicePeriod* this, ServiceId service_id);

ServicePeriod*
spRewind( ServicePeriod* this );

ServicePeriod*
spFastForward( ServicePeriod* this );

void
spPrint( ServicePeriod* this ) ;

void
spPrintPeriod( ServicePeriod* this ) ;

inline long
spNormalizeTime( ServicePeriod* this, int timezone_offset, long time ) ;

struct Timezone {
    TimezonePeriod* head;
} ; 

struct TimezonePeriod {
  long begin_time; //the first second on which the service_period is valid
  long end_time;   //the last second on which the service_period is valid
  int utc_offset;
  TimezonePeriod* next_period;
} ;

Timezone*
tzNew( );

void
tzAddPeriod( Timezone* this, TimezonePeriod* period );

TimezonePeriod*
tzPeriodOf( Timezone* this, long time);

int
tzUtcOffset( Timezone* this, long time);

TimezonePeriod*
tzHead( Timezone* this );

void
tzDestroy( Timezone* this );

TimezonePeriod*
tzpNew( long begin_time, long end_time, int utc_offset );

void
tzpDestroy( TimezonePeriod* this );

int
tzpUtcOffset( TimezonePeriod* this );

long
tzpBeginTime( TimezonePeriod* this );

long
tzpEndTime( TimezonePeriod* this );

TimezonePeriod*
tzpNextPeriod(TimezonePeriod* this);

#endif
