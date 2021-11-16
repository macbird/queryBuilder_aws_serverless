const AWS = require('aws-sdk');
const sqs = require('../lib/sqs');
AWS.config.setPromisesDependency(require('bluebird'));
AWS.config.update({ region: process.env.REGION });
const s3 = new AWS.S3();
const FileType = require('file-type');
const { URL } = require('url');
const merge = require('deepmerge')
const {Op, literal, col, fn, where} = require('sequelize')
const sequelize = require('sequelize')
const db = require('@libs/sequelize/models')
const _ = require('lodash');


const GM = require('gm');
const gm = GM.subClass({ imageMagick: true });
const axios = require('axios');

exports.queryBuilder = ({db, event, where, ids, path}) =>{
    var params = {}
    if(event.queryStringParameters){
        var {
            size, page, direction, order, field, groupBy,includes_required,
            filter, includes, searchIn,fieldToSearch, subQuery, select, ...filtros
        } = event.queryStringParameters
    }

    if(groupBy) params['group']=groupBy
    
    if(includes){
        var _includes = []
        includes.split(',').map(i=>i.trim()).map(i=>{
            
            if(i.includes('_as_')){
              var [model, alias] = i.split('_as_')
            }else{
              var model = i, alias = i
            }

            var inc = {
                model: db[Object.keys(db).find(key => key.toLowerCase() === model.replace(/(.+)s$/, '$1').toLowerCase())],
                as: alias,
                required:false
            }

            if(includes_required){
              var requireds = includes_required.split(',').map(i=>i.trim()).map(i=>{
                var [_include, required] = i.split(':')
                return {
                  alias:_include,
                  required:JSON.parse(required)
                }
              })
              console.log("requireds",requireds)
              var req = requireds.find(r=>r.alias == alias)
              console.log("req",req)
              if(req) inc['required'] = req.required
            }

            if(event.queryStringParameters[alias]){
                //var _attributes = event.queryStringParameters[alias].split(',').map(i=>i.trim())
                var _att = getSelect(event.queryStringParameters[alias])
                console.log("atributes",_att)
                inc['attributes'] = _att.attributes
                delete filtros[alias]
            }

            if(ids) inc['attributes'] = {
                include:['id']
            }
            console.log("include",inc)
            _includes.push(inc)
        })
        params['include'] = _includes
    }

    if(select){
        var _att = getSelect(select)
        if(!params.attributes) {
            params = {
                ...params,
                ..._att
            }
        }else{
            params.attributes = _att
        }
    }
          
    if (page) params['offset'] = parseInt((page-1)*size)
    
    if (size) params['limit'] = parseInt(size)
    
    if(field && direction)  {
        if(field.includes('.')){
            var [model,_field] = field.split('.')
            var include = params.include.find(i=>i.as.toLowerCase()==model.toLowerCase())
            console.log("include", include)
            params['order'] = [
              [
                {
                    model:include.model,
                    as: include.as
                },
                _field,
                direction
              ]
            ]
        }else{
            params['order'] = [[field, direction]]
        }
    }

    if(order)  {
      var _order = order.split(',').map(o=>o.trim())
      params['order'] = _order.map(o=>{
        var [field, direction] = o.split('_')
        if(field.includes('.')){
            var [model,_field] = field.split('.')
            var include = params.include.find(i=>i.as.toLowerCase()==model.toLowerCase())
            console.log("include", include)
            return [
              [
                {
                    model:include.model,
                    as: include.as
                },
                _field,
                direction
              ]
            ]
        }else{
            return [[field, direction]]
        }
      })
    }

    if (filter && searchIn) {
        searchIn = searchIn
        .toString()
        .split(",")
        .map((m) => {
          if(m.includes('_as_')) {
            if(m.includes('.')) {
              m =  m.split('_as_')[0] +"."+ m.split(".")[1]
            }else{
              m = m.split('_as_')[0]
            }
          }

          return m.trim()
        });

        params = merge(params, {
          where: {
            [Op.or]: searchIn.reduce((a,s) => {
              // var format = /[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]+/;
              // if(format.test(s)){
              //   a.push(literal(`REPLACE(REPLACE(REPLACE(${db[path].rawAttributes[s].field},'.', ''),'-', ''),'/', '') COLLATE Latin1_General_CI_AI LIKE '%${filter.replaceAll('.','').replaceAll('-','').replaceAll('/','').replace("'", "''")}%'`))
              // }else{
                if(s.includes('.')){
                  a.push(
                    {
                      [`$${s}$`]: 
                      {
                        [Op.like]: `%${filter}%`
                      }
                    }, 
                  )
                  
                }else{
                  a.push(
                    {
                      [s]: 
                      {
                        [Op.like]: `%${filter}%`
                      }
                    }, 
                  )
                }
              // }
              return a
            },[])
          }
        });
    }
   
    if(where){
        if(!params.where) {
            params = {
                ...params,
                ...{
                    where:where
                }
            }
        }else{
            params.where = where
        }
    }

    if (filtros) {
      delete filtros.column_aggregate
      Object.entries(filtros).map((entry) => {
        var [key, value] = entry;
        var _props = key.split('.')
        if(includes) var _associations = includes.split(',').map(i=>i.trim())
        if(_associations && _props.some(i=>_associations.includes(i))){
          var [, _include, ..._field] = _props
          var filtro = _.set({}, _field, value);
          if(_include.includes('_as_')) _include = _include.split('_as_')[1] 
          console.log("filtro",filtro)
          console.log("_include",_include)
          var where = _.find(params.include, { as: _include })['where']
          
          where = merge(where,replaceOperator(filtro))
          _.find(params.include, { as: _include })['where'] = where

        }else{
          var { filtro } = _.set({}, key, value);
          if (filtro.customFilter) delete filtro.customFilter;
          filtro = replaceOperator(filtro);
          params = merge(params, {
            where: filtro
          });
        }
      
      })
    }
    
    if(subQuery) params['subQuery'] = JSON.parse(subQuery)
    return params
}

// redimensiona imagem
exports.resize = async (buf, width, height) => {
  console.log("is buffer", buf)
  return new Promise((resolve, reject) => {
    gm(buf).strip().compress("JPEG").quality(85).resize(width, height).noProfile().toBuffer((err, buffer) => err ? reject(err) : resolve(buffer));
  });
};


function replaceOperator(filtro) {
  return Object.entries(filtro).reduce((obj, entry) => {
    var [key, value] = entry;

    if(key.includes('literal')){
      return {[Op.and]:[literal(value)]}
    }

    if (typeof value === "object") value = replaceOperator(value);

    if(key == 'like'){
      value = `%${value}%`
    }
    
    if(value && typeof value == 'string' && value.includes('col_')){
      var [func,expression] = value.split("_") 
      value = {[Op.col]:expression}
    }
    
    if(value && typeof value == 'string' && value.includes('fn_')){
      var [,func,expression] = value.split("_") 
      console.log("func",func)
      console.log("expression",expression)
      value = db.Sequelize[func](`${expression}`)
    }
    obj = {
      ...obj,
      ...{
        [Op[key] || key]: value
      }
    };
    return obj;
  }, {});
}


exports.checkPayloadIncludes = (payload, event) =>{
    var _obj = Object.keys(payload).reduce((acc, o) => {
        if(payload[o] && (typeof payload[o] == 'object')) {
            acc.push(o)
        }
        return acc
      }, [])
    if(_obj.length>0 && (!event.queryStringParameters || !event.queryStringParameters.includes)){
        throw Error("É nescessário passar o paramentro include")
    }else{
        if(_obj.length>0){
            _obj.map(o=>{
                var include = event.queryStringParameters.includes.split(",").map(i=>i.trim()).find(i=>i==o)
                if(!include) throw Error(`Você deve incluir ${o} no parametro includes`)
            })
        }
    }
}
