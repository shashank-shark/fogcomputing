import React from "react";

import {
    Position,
    Menu,
    MenuItem,
    MenuDivider,
    Popover,
    Icon, Button, Colors, Intent
} from '@blueprintjs/core';
import {Link} from "react-router-dom";

const UserSettingsMenuItems = () => {

    return (
        <Menu>
            <Menu>

                <Link to="/networksettings" style={{ textDecoration: 'none', color: 'black'}}>
                    <MenuItem text="Network Settings" icon="globe-network" />
                </Link>

                <MenuItem text="Update Settings" icon="automatic-updates" />
                <MenuDivider />
                <MenuItem text="Export Settings" icon="send-to" />
            </Menu>
        </Menu>
    );
};

const UserSettingsMenu = (props) => {

    return (
        <Popover content={<UserSettingsMenuItems />} position={Position.BOTTOM_LEFT}>
            <Button style={styles.iconStyles}>
                <Icon style={styles.innstyle} icon="cog" color={Colors.BLACK} />
            </Button>
        </Popover>
    );

};

const styles = {

    iconStyles : {
        margin: '2px'
    },
    innstyle: {
        margin: '2px'
    }
}

export default UserSettingsMenu;