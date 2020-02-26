import React from 'react';

import {Route, Switch, useRouteMatch} from "react-router-dom";

import RepositoryList from "./RepositoryList";
import RepositoryCreate from "./RepositoryCreate";
import BranchList from "./BranchList";
import BranchCreate  from "./BranchCreate";
import BranchExplorer from "./BranchExplorer";


export const ApiExplorer = () => {
    let { path } = useRouteMatch();

    return (
        <Switch>
            <Route exact path={path}>
                <RepositoryList/>
            </Route>
            <Route path={`${path}/create`}>
                <RepositoryCreate/>
            </Route>
            <Route path={`${path}/:repoId/branches/create`}>
                <BranchCreate/>
            </Route>
            <Route path={`${path}/:repoId/branch/:branchId`}>
                <BranchExplorer/>
            </Route>
            <Route exact path={`${path}/:repoId`}>
                <BranchList/>
            </Route>
        </Switch>
    );
};