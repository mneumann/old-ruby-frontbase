/************************************************

frontbase.c -

Author: Cail Borrell (cail@frontbase.com)
created at: Mon Jan 6 02:38:00 CET 2004

************************************************/

#include "ruby.h"
#include "rubyio.h"

#include <FBCAccess/FBCAccess.h>

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

struct fbsqlconnect
{
   int port;
   char *host;
   char *database;
   char *user;
   char *password;
   char *databasePassword;
   
   FBCExecHandler *fbeh;
   FBCDatabaseConnection *fbdc;
   FBCMetaData *meta;
};

struct fbsqlresult
{
   int rows;
   int cols;

   void **row;
   void *rawData;
   
   FBCDatabaseConnection *fbdc;
   FBCMetaData *md, *meta;
   FBCRowHandler* rowHandler;
   char* fetchHandle;
   int resultCount;
   int currentResult;
   int rowIndex;

   int currentRow;
};

struct fbsqllob
{
   FBCDatabaseConnection *fbdc;
   FBCBlobHandle *handle;
   char* bhandle;
   int size;
   int type;
};

typedef struct fbsqlconnect FBSQL_Connect;
typedef struct fbsqlresult FBSQL_Result;
typedef struct fbsqllob FBSQL_LOB;

static VALUE rb_cFBConn;
static VALUE rb_cFBResult;
static VALUE rb_cFBLOB;
static VALUE rb_cFBError;

static VALUE fbconn_query _((VALUE, VALUE));

static void free_fbresult _((FBSQL_Result*));
static int fetch_fbresult _((FBSQL_Result*, int, int, VALUE*));
static VALUE fbresult_result _((VALUE));
static VALUE fbresult_clear _((VALUE));
static VALUE fbresult_query _((VALUE));

static void free_fblob _((FBSQL_LOB*));

static FBSQL_Connect* get_fbconn _((VALUE));
static FBSQL_Result* get_fbresult _((VALUE));
static FBSQL_LOB* get_fblob _((VALUE));
static VALUE checkMetaData _((FBCDatabaseConnection*, FBCMetaData*));

EXTERN VALUE rb_mEnumerable;

#define FRONTBASE_COMMAND_OK 1
#define FRONTBASE_ROWS_OK    2
#define FRONTBASE_UNIQUE_OK  3

#define FB_ERR_NO_CONNECTION 1

#define FETCH_SIZE 4096

static void free_fbconn(ptr) FBSQL_Connect *ptr;
{
   fbcdcClose(ptr->fbdc);
   free(ptr);
}

static VALUE fbconn_connect(argc, argv, fbconn) int argc; VALUE *argv; VALUE fbconn;
{
   VALUE arg[6];
   FBSQL_Connect *conn = malloc(sizeof(FBSQL_Connect));

   conn->port = -1;
   conn->fbeh = NULL;
   rb_scan_args(argc, argv, "06", &arg[0], &arg[1], &arg[2], &arg[3], &arg[4], &arg[5]);

   if (!NIL_P(arg[0])) {
      Check_Type(arg[0], T_STRING);
      conn->host = STR2CSTR(arg[0]);
   }
   else {
      conn->host = "localhost";
   }
   if (!NIL_P(arg[1])) {
      conn->port = NUM2INT(arg[1]);
   }
   if (!NIL_P(arg[2])) {
      Check_Type(arg[2], T_STRING);
      conn->database = STR2CSTR(arg[2]);
   }
   if (!NIL_P(arg[3])) {
      Check_Type(arg[3], T_STRING);
      conn->user = STR2CSTR(arg[3]);
      
   }
   if (!NIL_P(arg[4])) {
      Check_Type(arg[4], T_STRING);
      conn->password = STR2CSTR(arg[4]);
   }
   if (!NIL_P(arg[5])) {
      Check_Type(arg[5], T_STRING);
      conn->databasePassword = STR2CSTR(arg[5]);
   }

   fbcInitialize();
   
   if (conn->port!=-1) {
      conn->fbdc = fbcdcConnectToDatabaseUsingPort(conn->host, conn->port, conn->databasePassword);
   }
   else {
      conn->fbdc = fbcdcConnectToDatabase(conn->database, conn->host, conn->databasePassword);
   }

   if (conn->fbdc == NULL) {
      rb_raise(rb_cFBError, fbcdcClassErrorMessage());
   }
     
   conn->meta = fbcdcCreateSession(conn->fbdc, "Ruby::DBI", conn->user, conn->password, "system_user");

   if (fbcmdErrorsFound(conn->meta) == T_TRUE) {
      FBCErrorMetaData* emd = fbcdcErrorMetaData(conn->fbdc, conn->meta);
      char* msgs = fbcemdAllErrorMessages(emd);

      rb_raise(rb_cFBError, msgs);
      fbcemdRelease(emd);
      free(msgs);

      fbcmdRelease(conn->meta);
      conn->meta = NULL;

      fbcdcClose(conn->fbdc);
      fbcdcRelease(conn->fbdc);
      conn->fbdc = NULL;

      return 0;
   }
   fbcmdRelease(conn->meta);
   conn->meta = NULL;

   return Data_Wrap_Struct(fbconn, 0, free_fbconn, conn);
}

static VALUE fbconn_close(obj) VALUE obj;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   
   if (!fbcdcConnected(conn->fbdc))
      rb_raise(rb_cFBError, "connection already closed.");

   if (conn->meta)
      fbcmdRelease(conn->meta);
   conn->meta = NULL;

   if (conn->fbdc) {
      fbcdcClose(conn->fbdc);
      fbcdcRelease(conn->fbdc);
   }
   conn->fbdc = NULL;

   if (conn->fbeh)
      fbcehRelease(conn->fbeh);
   conn->fbeh = NULL;

   if (conn->host) {
      conn->host = NULL;
   }
   if (conn->database) {
      conn->database = NULL;
   }
   if (conn->user) {
      conn->user = NULL;
   }
   if (conn->password) {
      conn->password = NULL;
   }
   if (conn->databasePassword) {
      conn->databasePassword = NULL;
   }
   
   DATA_PTR(obj) = 0;
   return Qnil;
}

static VALUE fbconn_autocommit(obj, commit) VALUE obj; int commit;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBCMetaData*   md;
   int i = NUM2INT(commit);

   if (conn->fbdc) {
      if (i)
         md = fbcdcExecuteDirectSQL(conn->fbdc,"SET COMMIT TRUE;");
      else
         md = fbcdcExecuteDirectSQL(conn->fbdc,"SET COMMIT FALSE;");

      checkMetaData(conn->fbdc, md);
      if (md)
         fbcmdRelease(md);
   }
   else
      rb_raise(rb_cFBError, "No connection available");
      
   return Qnil;
}

static VALUE fbconn_database_server_info(obj) VALUE obj;
{
   VALUE ret;
   VALUE result = fbconn_query(obj, rb_tainted_str_new2("VALUES(SERVER_NAME);"));
   fetch_fbresult(get_fbresult(result), 0, 0, &ret);
   fbresult_clear(result);
   
   return ret;
}

static VALUE fbconn_commit(obj) VALUE obj;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBCMetaData*   md;

   md = fbcdcCommit(conn->fbdc);
   checkMetaData(conn->fbdc, md);
   return Qnil;
}
   
static VALUE fbconn_rollback(obj) VALUE obj;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBCMetaData*   md;

   md = fbcdcRollback(conn->fbdc);
   checkMetaData(conn->fbdc, md);
   return Qnil;
}

static VALUE fbconn_query(obj, str) VALUE obj, str;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBSQL_Result *result = malloc(sizeof(FBSQL_Result));
   FBCMetaData *meta;

   result->fbdc = conn->fbdc;
   
   int status = FRONTBASE_COMMAND_OK;
   const char *msg, *type;
   char *sql, *sqlCmd;
   unsigned len;

   Check_Type(str, T_STRING);

   sql = STR2CSTR(str);
   len = strlen(sql);

   sqlCmd = malloc(len + 1 + 1);

   sprintf(sqlCmd, "%s", sql);
   if (sql[len-1] != ';')
      strcat(sqlCmd, ";");
   
   meta = fbcdcExecuteDirectSQL(conn->fbdc, sqlCmd);

   checkMetaData(conn->fbdc, meta);

   result->currentResult = 0;
   result->resultCount = 1;
   
   if (fbcmdHasMetaDataArray(meta)) {
      result->resultCount = fbcmdMetaDataArrayCount(meta);
      result->md = (FBCMetaData*) fbcmdMetaDataAtIndex(meta, 0);
      result->meta = meta;
   }
   else {
      result->md = meta;
      result->meta = meta;
   }
   
   type = fbcmdStatementType(result->md);

   if (type != NULL && strcmp("SELECT", type) == 0) {
      status = FRONTBASE_ROWS_OK;
   } else if(type != NULL && strcmp("UNIQUE", type) == 0) {
      status = FRONTBASE_UNIQUE_OK;
   }

   switch (status) {
      case FRONTBASE_COMMAND_OK:
      case FRONTBASE_ROWS_OK:
      case FRONTBASE_UNIQUE_OK:
         result->row = NULL;
         result->rawData = NULL;
         result->rowHandler = NULL;
         result->fetchHandle = fbcmdFetchHandle(result->meta);
         result->rows = fbcmdRowCount(result->meta);
         result->cols = fbcmdColumnCount(result->meta);
         result->rowIndex = -1;
         return Data_Wrap_Struct(rb_cFBResult, 0, free_fbresult, result);

      default:
         msg = fbcdcErrorMessage(conn->fbdc);
         break;
   }

   fbcmdRelease(result->meta);
   rb_raise(rb_cFBError, msg);
}

static VALUE fbconn_exec(obj, str) VALUE obj, str;
{
   VALUE result = fbconn_query(obj, str);
   fbresult_clear(result);
   return result;
}

static VALUE fbconn_host(obj) VALUE obj;
{
   const char *host = fbcdcHostName(get_fbconn(obj)->fbdc);
   if (!host) return Qnil;
   return rb_tainted_str_new2(host);
}

static VALUE fbconn_db(obj) VALUE obj;
{
   const char *db = fbcdcDatabaseName(get_fbconn(obj)->fbdc);
   if (!db) return Qnil;
   return rb_tainted_str_new2(db);
}

static VALUE fbconn_user(obj) VALUE obj;
{
   return rb_tainted_str_new2(get_fbconn(obj)->user);
}

static VALUE fbconn_status(obj) VALUE obj;
{
   Bool status = fbcdcConnected(get_fbconn(obj)->fbdc);

   return INT2NUM(status ? 1 : 0);
}

static VALUE fbconn_error(obj) VALUE obj;
{
   const char *error = fbcdcErrorMessage(get_fbconn(obj)->fbdc);
   if (!error) return Qnil;
   return rb_tainted_str_new2(error);
}

static VALUE fbconn_create_blob(VALUE obj, VALUE data)
{
   int size;

   FBSQL_Connect *conn = get_fbconn(obj);
   FBSQL_LOB *lob = malloc(sizeof(FBSQL_LOB));
   size = RSTRING(data)->len;

   lob->type = FB_BLOB;
   lob->fbdc = conn->fbdc;
   lob->bhandle = NULL;
   lob->handle = fbcdcWriteBLOB(conn->fbdc, RSTRING(data)->ptr, size);
   lob->size = size;

   return Data_Wrap_Struct(rb_cFBLOB, 0, free_fblob, lob);
}

static VALUE fbconn_create_clob(VALUE obj, VALUE data)
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBSQL_LOB *lob = malloc(sizeof(FBSQL_LOB));

   lob->type = FB_CLOB;
   lob->fbdc = conn->fbdc;
   lob->bhandle = NULL;
   lob->handle = fbcdcWriteCLOB(conn->fbdc, RSTRING(data)->ptr);
   lob->size = RSTRING(data)->len;

   return Data_Wrap_Struct(rb_cFBLOB, 0, free_fblob, lob);
}

static void free_fbresult(ptr) FBSQL_Result *ptr;
{
   fbcmdRelease(ptr->meta);
   free(ptr);
}

static int fetch_fbresult(FBSQL_Result *result, int row_index, int column_index, VALUE *r)
{
   char* value;
   int length;

   if (!result->meta)
      rb_raise(rb_cFBError, "No result to fetch.");

   if (row_index < 0)
      rb_raise(rb_cFBError, "Invalid row number.");

   if (result->rawData == NULL || result->currentRow >= fbcrhRowCount(result->rowHandler)) {
      result->rawData = fbcdcFetch(result->fbdc, FETCH_SIZE, result->fetchHandle);

      if (result->rawData == NULL)
         return -1;
      
      if (result->rowHandler != NULL)  {
         fbcrhRelease(result->rowHandler);
      }

      result->rowHandler = fbcrhInitWith(result->rawData, result->md);
      result->currentRow = -1;

      if (result->rowHandler == NULL)  {
         return -1;
      }
   }

   if (result->rowIndex != row_index) {
      result->rowIndex = row_index;
      result->currentRow++;
      result->row = fbcrhRowAtIndex(result->rowHandler, result->currentRow);
   }


   if (!result->row) {
      return -1;
   }

   const FBCDatatypeMetaData *dtmd = fbcmdDatatypeMetaDataAtIndex(result->md, column_index);
   unsigned dtc = fbcdmdDatatypeCode(dtmd);

   if (result->row[column_index] == NULL) {
      *r = Qnil;
      return 1;
   }
   else {
      switch(dtc)
      {
         case FB_Boolean:
         {
            switch(*(char*) result->row[column_index]) {
               case 0:		value = strdup("FALSE");	break;
               case 1:		value = strdup("TRUE");		break;
               default: 	value = strdup("NULL");		break;
            }
            break;
         }
         case FB_PrimaryKey:
         case FB_Integer:
         {
            char	buf[100];
            sprintf(buf, "%d", *((long*) result->row[column_index]));
            value	= strdup(buf);
            break;
         }
         case FB_SmallInteger:
         {
            char	buf[100];
            sprintf(buf, "%d", *(short *) result->row[column_index]);
            value	= strdup(buf);
            break;
         }
         case FB_Float:
         case FB_Real:
         case FB_Double:
         case FB_Numeric:
         case FB_Decimal:
         {
            char	buf[100];
            sprintf(buf, "%f", *(double *) result->row[column_index]);
            value	= strdup(buf);
            break;
         }
         case FB_Character:
         case FB_VCharacter:
         {
            value	= strdup((char*) result->row[column_index]);
            break;
         }
         case FB_Bit:
         case FB_VBit:
         {
            const FBCColumnMetaData* clmd  =  fbcmdColumnMetaDataAtIndex(result->md, column_index);
            struct bitValue
            {
               unsigned int   nBytes;
               unsigned char* bytes;
            };
            struct bitValue*  ptr = result->row[column_index];
            unsigned nBits = ptr->nBytes * 8;

            if (dtc == FB_Bit) nBits = fbcdmdLength(fbccmdDatatype(clmd));
            if (nBits % 8 == 0) {
               unsigned i;
               unsigned int l = nBits / 8;
               length = l*2+3+1;
               value = malloc(length);
               value[0] = 'X';
               value[1] = '\'';
               for (i = 0; i < nBits / 8; i++)
               {
                  char c[4];
                  sprintf(c,"%02x",ptr->bytes[i]);
                  value[i*2+2] = c[0];
                  value[i*2+3] = c[1];
               }
               value[i*2+2] = '\'';
               value[i*2+3] = 0;
            }
            else {
               unsigned i;
               unsigned int l = nBits;
               length = l+3+1;
               value = malloc(length);
               value[0] = 'B';
               value[1] = '\'';
               for (i = 0; i < nBits; i++)
               {
                  int bit = 0;
                  if (i/8 < ptr->nBytes) bit = ptr->bytes[i/8] & (1<<(7-(i%8)));
                  value[i*2+2] = bit?'1':'0';
               }
               value[i*2+2] = '\'';
               value[i*2+3] = 0;
            }
            break;
         }
         case FB_BLOB:
         case FB_CLOB:
         {
            unsigned char* bytes = (unsigned char*) result->row[column_index];
            FBSQL_LOB *lob = malloc(sizeof(FBSQL_LOB));

            lob->type = dtc;
            lob->fbdc = result->fbdc;
            lob->bhandle = strdup(&bytes[1]);
            lob->handle = fbcbhInitWithHandle(lob->bhandle);
            lob->size = fbcbhBlobSize(lob->handle);

            *r = Data_Wrap_Struct(rb_cFBLOB, 0, free_fblob, lob);
            return 1;
         }
         case FB_Date:
         case FB_Time:
         case FB_TimeTZ:
         case FB_Timestamp:
         case FB_TimestampTZ:
         {
            value = strdup((char*) result->row[column_index]);
            break;
         }
         case FB_YearMonth:
         {
            value = "YearMonth";
            break;
         }
         case FB_DayTime:
         {
            value = "DayTime";
            break;
         }
         default:
            rb_raise(rb_cFBError, "Undefined column type.");
      }
   }
   *r = rb_tainted_str_new2(value);
   return 1;
}

static VALUE fbresult_status(obj) VALUE obj;
{
   FBSQL_Result *result;
   int status = FRONTBASE_COMMAND_OK;
   char *type;

   result = get_fbresult(obj);

   if (fbcmdErrorsFound(result->meta) == T_TRUE)
      return -1;
   
   type = fbcmdStatementType(result->meta);

   if (type != NULL && strcmp("SELECT", type) == 0) {
      status = FRONTBASE_ROWS_OK;
   } else if (type != NULL && strcmp("UNIQUE", type) == 0) {
      status = FRONTBASE_UNIQUE_OK;
   }

   return INT2NUM(status);
}

static VALUE fbresult_result(obj) VALUE obj;
{
   FBSQL_Result *result;
   VALUE ary, value;
   int i, j, k;
   i = 0;
   result = get_fbresult(obj);
   ary = rb_ary_new();

   if (fbcmdFetchHandle(result->meta) == NULL)
      return ary;
   
   while (1) {
   //for (i=0; i<result->rows; i++) {
      VALUE row = rb_ary_new2(result->cols);
      for (j=0; j<result->cols; j++) {
         k = fetch_fbresult(result, i, j, &value);
         if (k != -1)
            rb_ary_push(row, value);
         else
            return ary;
      }
      rb_ary_push(ary, row);
      i++;
   };

   return ary;
}

static VALUE fbresult_each(obj) VALUE obj;
{
   FBSQL_Result *result;
   int i, j;
   VALUE value;

   result = get_fbresult(obj);
   
   for (i=0; i<result->rows; i++) {
      VALUE row = rb_ary_new2(result->cols);
      for (j=0; j<result->cols; j++) {
         fetch_fbresult(result, i, j, &value);
         rb_ary_push(row, value);
      }
      rb_yield(row);
   };

   return Qnil;
}

static VALUE fbresult_aref(argc, argv, obj) int argc; VALUE *argv; VALUE obj;
{
   FBSQL_Result *result;
   VALUE a1, a2, val, value;
   int i, j;

   result = get_fbresult(obj);

   switch (rb_scan_args(argc, argv, "11", &a1, &a2)) {
      case 1:
         i = NUM2INT(a1);
         if( i >= result->rows ) return Qnil;

            val = rb_ary_new();
         for (j=0; j<result->cols; j++) {
            fetch_fbresult(result, i, j, &value);
            rb_ary_push(val, value);
         }
            return val;

      case 2:
         i = NUM2INT(a1);
         if( i >= result->rows ) return Qnil;
         j = NUM2INT(a2);
         if( j >= result->cols ) return Qnil;

         fetch_fbresult(result, i, j, &value);
         return value;

      default:
         return Qnil;		/* not reached */
   }
}

static VALUE fbresult_columns(obj) VALUE obj;
{
   FBSQL_Result *result;
   const FBCColumnMetaData *column_meta;
   VALUE ary;
   int i;

   result = get_fbresult(obj);
   ary = rb_ary_new2(result->cols);
   
   for (i=0;i<result->cols;i++) {
      column_meta = fbcmdColumnMetaDataAtIndex(result->meta, i);
      rb_ary_push(ary, rb_tainted_str_new2(fbccmdLabelName(column_meta)));
   }
   
   return ary;
}

static VALUE fbresult_num_rows(obj) VALUE obj;
{
   return INT2NUM(get_fbresult(obj)->rows);
}

static VALUE fbresult_num_cols(obj) VALUE obj;
{
   return INT2NUM(get_fbresult(obj)->cols);
}

static VALUE fbresult_column_name(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCColumnMetaData *column_meta;
   int i = NUM2INT(index);

   result = get_fbresult(obj);

   if (i < 0 || i >= result->cols) {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   column_meta = fbcmdColumnMetaDataAtIndex(result->meta, i);
   return rb_tainted_str_new2(fbccmdLabelName(column_meta));
}

static VALUE fbresult_column_type(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCDatatypeMetaData *datatype_meta;
   
   int i = NUM2INT(index);
   int type;

   result = get_fbresult(obj);
   datatype_meta = fbcmdDatatypeMetaDataAtIndex(result->meta, i);
   
   if (i < 0 || i >= result->cols) {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   if (datatype_meta) {
      type = fbcdmdDatatypeCode(datatype_meta);
   }

   return INT2NUM(type);
}

static VALUE fbresult_column_length(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCDatatypeMetaData *datatype_meta;

   int i = NUM2INT(index);
   int size;

   result = get_fbresult(obj);
   datatype_meta = fbcmdDatatypeMetaDataAtIndex(result->meta, i);
   
   if (i < 0 || i >= result->cols) {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   if (datatype_meta) {
      size = fbcdmdLength(datatype_meta);
   }
   
   return INT2NUM(size);
}

static VALUE fbresult_column_precision(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCDatatypeMetaData *datatype_meta;

   int i = NUM2INT(index);
   int size;

   result = get_fbresult(obj);
   datatype_meta = fbcmdDatatypeMetaDataAtIndex(result->meta, i);

   if (i < 0 || i >= result->cols) {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   if (datatype_meta) {
      size = fbcdmdPrecision(datatype_meta);
   }

   return INT2NUM(size);
}

static VALUE fbresult_column_scale(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCDatatypeMetaData *datatype_meta;

   int i = NUM2INT(index);
   int size;

   result = get_fbresult(obj);
   datatype_meta = fbcmdDatatypeMetaDataAtIndex(result->meta, i);

   if (i < 0 || i >= result->cols) {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   if (datatype_meta) {
      size = fbcdmdScale(datatype_meta);
   }

   return INT2NUM(size);
}

static VALUE fbresult_column_isnullable(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCColumnMetaData *column_meta;
   int i = NUM2INT(index);

   result = get_fbresult(obj);

   if (i < 0 || i >= result->cols) {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   column_meta = fbcmdColumnMetaDataAtIndex(result->meta, i);
   return fbccmdIsNullable(column_meta)? Qtrue : Qfalse;
}

static VALUE fbresult_clear(obj) VALUE obj;
{
   FBSQL_Result *result = get_fbresult(obj);

   if (result->meta) {
      fbcmdRelease(result->meta);
   }
   result->meta = NULL;
   result->md = NULL;

   if (result->fbdc) {
      result->fbdc = NULL;
   }

   if (result->rowHandler != NULL)  {
      fbcrhRelease(result->rowHandler);
   }

   result->row = NULL;
   result->rawData = NULL;
   result->fetchHandle = NULL;

   DATA_PTR(obj) = 0;
   return Qnil;
}

static void free_fblob(ptr) FBSQL_LOB *ptr;
{
   ptr->fbdc = NULL;
   ptr->bhandle = NULL;
   if (ptr->handle != NULL)
      fbcbhRelease(ptr->handle);
   ptr->handle = NULL;
   ptr->size = NULL;
   free(ptr);
}

static VALUE fblob_read(obj) VALUE obj;
{
   FBSQL_LOB *lob = get_fblob(obj);

   if (lob->type == FB_BLOB)
      return rb_tainted_str_new((char*) fbcdcReadBLOB(lob->fbdc, lob->handle), lob->size);
   else
	return rb_tainted_str_new((char*) fbcdcReadCLOB(lob->fbdc, lob->handle), lob->size);
}

static VALUE fblob_handle(obj) VALUE obj;
{
   FBSQL_LOB *lob = get_fblob(obj);

   return rb_tainted_str_new2(fbcbhDescription(lob->handle));
}

static VALUE fblob_size(obj) VALUE obj;
{
   FBSQL_LOB *lob = get_fblob(obj);

   return INT2NUM(lob->size);
}

static FBSQL_LOB* get_fblob(obj) VALUE obj;
{
   FBSQL_LOB *lob;

   Data_Get_Struct(obj, FBSQL_LOB, lob);
   if (lob == 0)
      rb_raise(rb_cFBError, "no blob available");

   return lob;
}

static FBSQL_Connect* get_fbconn(obj) VALUE obj;
{
   FBSQL_Connect *conn;

   Data_Get_Struct(obj, FBSQL_Connect, conn);
   if (conn == 0)
      rb_raise(rb_cFBError, "closed connection");

   return conn;
}

static FBSQL_Result* get_fbresult(obj) VALUE obj;
{
   FBSQL_Result *result;

   Data_Get_Struct(obj, FBSQL_Result, result);
   if (result == 0)
      rb_raise(rb_cFBError, "no result available");

   return result;
}

static VALUE checkMetaData(conn, meta) FBCDatabaseConnection* conn; FBCMetaData* meta;
{
   int result = 1;

   if (meta == NULL)
   {
      rb_raise(rb_cFBError, "Connection to database server was lost.");
      result = 0;
   }
   else if (fbcmdErrorsFound(meta))
   {
      FBCErrorMetaData* emd = fbcdcErrorMetaData(conn, meta);
      char*             emg = fbcemdAllErrorMessages(emd);
      if (emg)
         rb_raise(rb_cFBError, emg);
      else
         rb_raise(rb_cFBError, "No message");

      free(emg);
      fbcemdRelease(emd);
      result = 0;
   }
   return result;
}

void Init_frontbase()
{
   rb_cFBError  = rb_define_class("FBError", rb_eStandardError);
   rb_cFBConn   = rb_define_class("FBSQL_Connect", rb_cObject);
   rb_cFBResult = rb_define_class("FBSQL_Result", rb_cObject);
   rb_cFBLOB   = rb_define_class("FBSQL_LOB", rb_cObject);

   rb_define_method(rb_cFBLOB, "read", fblob_read, 0);
   rb_define_method(rb_cFBLOB, "handle", fblob_handle, 0);
   rb_define_method(rb_cFBLOB, "size", fblob_size, 0);

   rb_define_singleton_method(rb_cFBConn, "new", fbconn_connect, -1);
   rb_define_singleton_method(rb_cFBConn, "connect", fbconn_connect, -1);
   rb_define_singleton_method(rb_cFBConn, "setdb", fbconn_connect, -1);
   rb_define_singleton_method(rb_cFBConn, "setdblogin", fbconn_connect, -1);

   rb_define_const(rb_cFBConn, "NO_CONNECTION", INT2FIX(FB_ERR_NO_CONNECTION));

   rb_define_method(rb_cFBConn, "create_blob", fbconn_create_blob, 1);
   rb_define_method(rb_cFBConn, "create_clob", fbconn_create_clob, 1);

   rb_define_method(rb_cFBConn, "database_server_info", fbconn_database_server_info, 0);
   rb_define_method(rb_cFBConn, "autocommit", fbconn_autocommit, 1);
   rb_define_method(rb_cFBConn, "commit", fbconn_commit, 0);
   rb_define_method(rb_cFBConn, "rollback", fbconn_rollback, 0);
   rb_define_method(rb_cFBConn, "db", fbconn_db, 0);
   rb_define_method(rb_cFBConn, "host", fbconn_host, 0);
   rb_define_method(rb_cFBConn, "status", fbconn_status, 0);
   rb_define_method(rb_cFBConn, "error", fbconn_error, 0);
   rb_define_method(rb_cFBConn, "close", fbconn_close, 0);
   rb_define_alias(rb_cFBConn, "finish", "close");
   rb_define_method(rb_cFBConn, "user", fbconn_user, 0);

   rb_define_method(rb_cFBConn, "exec", fbconn_exec, 1);
   rb_define_method(rb_cFBConn, "query", fbconn_query, 1);

   rb_include_module(rb_cFBResult, rb_mEnumerable);

   rb_define_const(rb_cFBResult, "COMMAND_OK", INT2FIX(FRONTBASE_COMMAND_OK));
   rb_define_const(rb_cFBResult, "ROWS_OK", INT2FIX(FRONTBASE_ROWS_OK));
   rb_define_const(rb_cFBResult, "UNIQUE_OK", INT2FIX(FRONTBASE_UNIQUE_OK));
   
   rb_define_const(rb_cFBConn, "COMMAND_OK", INT2FIX(FRONTBASE_COMMAND_OK));
   rb_define_const(rb_cFBConn, "ROWS_OK", INT2FIX(FRONTBASE_ROWS_OK));
   rb_define_const(rb_cFBConn, "UNIQUE_OK", INT2FIX(FRONTBASE_UNIQUE_OK));

   rb_define_const(rb_cFBConn, "FB_Undecided", INT2FIX(FB_Undecided));
   rb_define_const(rb_cFBConn, "FB_PrimaryKey", INT2FIX(FB_PrimaryKey));
   rb_define_const(rb_cFBConn, "FB_Boolean", INT2FIX(FB_Boolean));
   rb_define_const(rb_cFBConn, "FB_Integer", INT2FIX(FB_Integer));
   rb_define_const(rb_cFBConn, "FB_SmallInteger", INT2FIX(FB_SmallInteger));
   rb_define_const(rb_cFBConn, "FB_Float", INT2FIX(FB_Float));
   rb_define_const(rb_cFBConn, "FB_Real", INT2FIX(FB_Real));
   rb_define_const(rb_cFBConn, "FB_Double", INT2FIX(FB_Double));
   rb_define_const(rb_cFBConn, "FB_Numeric", INT2FIX(FB_Numeric));
   rb_define_const(rb_cFBConn, "FB_Decimal", INT2FIX(FB_Decimal));
   rb_define_const(rb_cFBConn, "FB_Character", INT2FIX(FB_Character));
   rb_define_const(rb_cFBConn, "FB_VCharacter", INT2FIX(FB_VCharacter));
   rb_define_const(rb_cFBConn, "FB_Bit", INT2FIX(FB_Bit));
   rb_define_const(rb_cFBConn, "FB_VBit", INT2FIX(FB_VBit));
   rb_define_const(rb_cFBConn, "FB_Date", INT2FIX(FB_Date));
   rb_define_const(rb_cFBConn, "FB_Time", INT2FIX(FB_Time));
   rb_define_const(rb_cFBConn, "FB_TimeTZ", INT2FIX(FB_TimeTZ));
   rb_define_const(rb_cFBConn, "FB_Timestamp", INT2FIX(FB_Timestamp));
   rb_define_const(rb_cFBConn, "FB_TimestampTZ", INT2FIX(FB_TimestampTZ));
   rb_define_const(rb_cFBConn, "FB_YearMonth", INT2FIX(FB_YearMonth));
   rb_define_const(rb_cFBConn, "FB_DayTime", INT2FIX(FB_DayTime));
   rb_define_const(rb_cFBConn, "FB_CLOB", INT2FIX(FB_CLOB));
   rb_define_const(rb_cFBConn, "FB_BLOB", INT2FIX(FB_BLOB));
   rb_define_const(rb_cFBConn, "FB_TinyInteger", INT2FIX(FB_TinyInteger));
   rb_define_const(rb_cFBConn, "FB_LongInteger", INT2FIX(FB_LongInteger));

   rb_define_method(rb_cFBResult, "status", fbresult_status, 0);
   rb_define_method(rb_cFBResult, "result", fbresult_result, 0);
   rb_define_method(rb_cFBResult, "each", fbresult_each, 0);
   rb_define_method(rb_cFBResult, "[]", fbresult_aref, -1);
   rb_define_method(rb_cFBResult, "columns", fbresult_columns, 0);
   rb_define_method(rb_cFBResult, "num_rows", fbresult_num_rows, 0);
   rb_define_method(rb_cFBResult, "num_cols", fbresult_num_cols, 0);
   rb_define_method(rb_cFBResult, "column_name", fbresult_column_name, 1);
   rb_define_method(rb_cFBResult, "column_type", fbresult_column_type, 1);
   rb_define_method(rb_cFBResult, "column_length", fbresult_column_length, 1);
   rb_define_method(rb_cFBResult, "column_precision", fbresult_column_precision, 1);
   rb_define_method(rb_cFBResult, "column_scale", fbresult_column_scale, 1);
   rb_define_method(rb_cFBResult, "column_isnullable", fbresult_column_isnullable, 1);
   rb_define_method(rb_cFBResult, "clear", fbresult_clear, 0);
   rb_define_method(rb_cFBResult, "close", fbresult_clear, 0);
}
