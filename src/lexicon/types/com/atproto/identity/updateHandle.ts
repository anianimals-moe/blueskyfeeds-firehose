/**
 * GENERATED CODE - DO NOT MODIFY
 */
import {Request, Response} from 'express'
import { HandlerAuth } from '@atproto/xrpc-server'

export interface QueryParams {}

export interface InputSchema {
  handle: string
  [k: string]: unknown
}

export interface HandlerInput {
  encoding: 'application/json'
  body: InputSchema
}

export interface HandlerError {
  status: number
  message?: string
}

export type HandlerOutput = HandlerError | void
export type Handler<HA extends HandlerAuth = never> = (ctx: {
  auth: HA
  params: QueryParams
  input: HandlerInput
  req: Request
  res: Response
}) => Promise<HandlerOutput> | HandlerOutput
